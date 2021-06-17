import { EMPTY, from, merge, Observable, of, Subject, timer } from 'rxjs'
import { catchError, concatMap, debounce, debounceTime, delay, finalize, map, skip, take, takeUntil, tap, throttleTime } from 'rxjs/operators'
import { Source } from './source/source'
import { Dump, SequenceStore, Result, Revision } from './sequence-store'
import { Store } from './store'
export { Operation } from 'fast-json-patch'

export class PatchDB<T extends object> {
  private readonly cancelStashTimeout = new Subject()

  private constructor (
    private readonly sources: Source<T>[],
    private readonly http: Http<T>,
    private readonly sequenceStore: SequenceStore<T>,
    private readonly timeoutForMissingRevision: number = 5000,
  ) { }

  get store (): Store<T> { return this.sequenceStore.store }

  static async init<T extends object> (conf: PatchDbConfig<T>): Promise<PatchDB<T>> {
    console.log('PATCHDB - init(): ', conf)
    const { sources, http, bootstrapper, timeoutForMissingRevision } = conf

    let sequence: number = 0
    let data: T = { } as T
    try {
      const cache = await bootstrapper.init()
      console.log('bootstrapped: ', cache)
      sequence = cache.sequence
      data = cache.data
    } catch (e) {
      // @TODO what to do if bootstrapper fails?
      console.error('bootstrapper failed: ', e)
    }

    const store = new Store(data)

    const sequenceStore = new SequenceStore(store, sequence)

    // update cache when sequenceStore emits, throttled
    sequenceStore.watch$().pipe(debounceTime(500), skip(1)).subscribe(({ data, sequence }) => {
      console.log('PATCHDB - update cache(): ', sequence, data)
      bootstrapper.update({ sequence, data }).catch(e => {
        console.error('Exception in updateCache: ', e)
      })
    })

    return new PatchDB(sources, http, sequenceStore, timeoutForMissingRevision)
  }

  sync$ (): Observable<void> {
    console.log('PATCHDB - sync$()')

    const sequence$ = this.sequenceStore.watch$().pipe(map(cache => cache.sequence))
    // nested concatMaps, as it is written, ensure sync is not run for update2 until handleSyncResult is complete for update1.
    // flat concatMaps would allow many syncs to run while handleSyncResult was hanging. We can consider such an idea if performance requires it.
    return merge(...this.sources.map(s => s.watch$(sequence$))).pipe(
      tap(update => console.log('PATCHDB - source updated:', update)),
      concatMap(update =>
        this.sequenceStore.update$(update).pipe(
          concatMap(res => this.handleSyncResult$(res)),
        ),
      ),
      finalize(() => {
        console.log('FINALIZING')
        this.sequenceStore.sequence = 0
      }),
    )
  }

  private handleSyncResult$ (res: Result): Observable<void> {
    console.log('PATCHDB - handleSyncResult$(): ', res)
    switch (res) {
      case Result.DUMPED: return of(this.cancelStashTimeout.next('')) // cancel stash timeout
      case Result.REVISED: return of(this.cancelStashTimeout.next('')) // cancel stash timeout
      case Result.STASHED: return this.handleStashTimeout$() // call error after timeout
      case Result.ERROR: return this.handlePatchError$() // call error immediately
      default: return EMPTY
    }
  }

  private handleStashTimeout$ (): Observable<void> {
    console.log('PATCHDB - handleStashTimeout$()')
    return timer(this.timeoutForMissingRevision).pipe(
      tap(time => console.log('PATCHDB - timeout for missing patch:', time)),
      takeUntil(this.cancelStashTimeout),
      take(1),
      concatMap(() => this.handlePatchError$()),
    )
  }

  // Here flattened concatMaps are functionally equivalent to nested because the source observable emits at most once.
  private handlePatchError$ (): Observable<void> {
    return from(this.http.getDump()).pipe(
      concatMap(dump => this.sequenceStore.update$(dump)),
      // note the above is a "dump" update, which will always return DUMPED (it can't error)
      // handleSyncResult will therefore never re-call handlePatchError()
      concatMap(res => this.handleSyncResult$(res)),
      catchError(e => {
        console.error(e)
        return EMPTY
      }),
    )
  }
}

export type PatchDbConfig<T> = {
  http: Http<T>
  sources: Source<T>[]
  bootstrapper: Bootstrapper<T>
  timeoutForMissingRevision?: number
}

export enum PatchOp {
  ADD = 'add',
  REMOVE = 'remove',
  REPLACE = 'replace',
}

export interface Http<T> {
  getRevisions (since: number): Promise<Revision[] | Dump<T>>
  getDump (): Promise<Dump<T>>
}

export interface Bootstrapper<T> {
  init (): Promise<DBCache<T>>
  update (cache: DBCache<T>): Promise<void>
}

export interface DBCache<T>{
  sequence: number,
  data: T
}
