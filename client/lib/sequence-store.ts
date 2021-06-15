import { BehaviorSubject, Observable } from 'rxjs'
import { filter } from 'rxjs/operators'
import { Store } from './store'
import { DBCache } from './patch-db'
import { patchDocument } from './store'
import { ActionSerializer } from './action-serializer'
import { Operation } from 'fast-json-patch'
import BTree from 'sorted-btree'

export class SequenceStore<T extends object> {
  private readonly lastState$: BehaviorSubject<DBCache<T>> = new BehaviorSubject(undefined as any)
  private readonly actionSerializer = new ActionSerializer()
  private preTemps: T
  private stashed = new BTree<number, Revision>()
  private temps: UpdateTemp[] = []
  private sequence$ = new BehaviorSubject(0)

  constructor (
    readonly store: Store<T>,
    initialSequence: number,
  ) {
    const data = store.peek
    this.preTemps = data
    this.commit({ data, sequence: initialSequence }, [])
  }

  get sequence (): number { return this.sequence$.getValue() }
  set sequence (seq: number) { this.sequence$.next(seq) }

  // subscribe to watch$ to get sequence + T feed, e.g. for caching and bootstrapping from a cache
  watch$ (): Observable<DBCache<T>> {
    return this.lastState$.pipe(filter(a => !!a))
  }

  update$ (update: Update<T>): Observable<Result> {
    return this.actionSerializer.run$(() => {
      if (isTemp(update)) {
        return this.updateTemp(update)
      } else {
        return this.updateReal(update)
      }
    })
  }

  viewRevisions (): Revision[] {
    // return this.revisions.filter(a => !!a)
    return this.stashed.valuesArray()
  }

  private updateReal (update: UpdateReal<T>): Result {
    if (update.expireId) { this.temps = this.temps.filter(temp => temp.expiredBy !== update.expireId) }
    if (update.id <= this.sequence) return Result.NOOP

    const { result, dbCache, revisionsToDelete } = isDump(update) ?
      this.dump(update) :
      this.revise(update)

    this.preTemps = dbCache.data
    const afterTemps = this.stageSeqTemps(dbCache)
    this.commit(afterTemps, revisionsToDelete)
    return result
  }

  private updateTemp (update: UpdateTemp): Result {
    this.temps.push(update)
    const data = patchDocument(update.patch, this.store.peek)
    const res = {
      data,
      sequence: this.sequence,
    }
    this.commit(res, [])
    return Result.TEMP
  }

  private commit (res: DBCache<T>, revisionsToDelete: number[]): void {
    const { data, sequence } = res
    this.stashed.deleteKeys(revisionsToDelete)
    this.sequence$.next(sequence)
    this.store.set(data)
    this.lastState$.next({ data, sequence })
  }

  private dump (dump: Dump<T>): { result: Result, dbCache: DBCache<T>, revisionsToDelete: number[] } {
    try {
      const oldRevisions = this.stashed.filter((key, _) => key < dump.id).keysArray()
      const { dbCache, revisionsToDelete } = this.processRevisions(dump.value, dump.id)
      return {
        result: Result.DUMPED,
        dbCache,
        revisionsToDelete: oldRevisions.concat(revisionsToDelete),
      }
    } catch (e) {
      console.error(`Dump error for ${JSON.stringify(dump)}: `, e)
      return {
        result: Result.ERROR,
        dbCache: {
          data: this.preTemps,
          sequence: this.sequence,
        },
        revisionsToDelete: [],
      }
    }
  }

  private revise (revision: Revision): { result: Result, dbCache: DBCache<T>, revisionsToDelete: number[] } {
    this.stashed.set(revision.id, revision)
    try {
      return this.processRevisions(this.preTemps, this.sequence)
    } catch (e) {
      console.error(`Revise error for ${JSON.stringify(revision)}: `, e)
      return {
        result: Result.ERROR,
        dbCache: {
          data: this.preTemps,
          sequence: this.sequence,
        },
        revisionsToDelete: [],
      }
    }
  }

  private stageSeqTemps<S extends DBCache<T>> (resultSoFar: S): S {
    return this.temps.reduce(({ data, ...rest }, nextTemp ) => {
      try {
        const nextContents = patchDocument(nextTemp.patch, data)
        return { data: nextContents, ...rest } as S
      } catch (e) {
        console.error(`Skipping temporary patch ${JSON.stringify(nextTemp)} due to exception: `, e)
        return { data, ...rest } as S
      }
    }, resultSoFar)
  }

  private processRevisions (data: T, sequence: number): { result: Result, dbCache: DBCache<T>, revisionsToDelete: number[] } {
    const applicableRevisions = this.applicableRevisions(sequence)

    console.log('APPLICABLE: ', applicableRevisions)

    if (!applicableRevisions.length) {
      return {
        result: Result.STASHED,
        dbCache: {
          data,
          sequence,
        },
        revisionsToDelete: [],
      }
    }

    const revisionsToDelete: number[] = []
    const toReturn = applicableRevisions.reduce(({ data, sequence }, revision) => {
      const nextContents = patchDocument(revision.patch, data)
      const nextSequence = sequence + 1
      revisionsToDelete.push(revision.id) // @TODO original was `revisionsToDelete.concat([seqPatch.id])`, why?
      return { data: nextContents, sequence: nextSequence }
    }, { data, sequence })

    return {
      result: Result.REVISED,
      dbCache: toReturn,
      revisionsToDelete,
    }
  }

  private applicableRevisions (sequence: number): Revision[] {
    const toReturn = [] as Revision[]

    let i = sequence
    while (true) {
      i++
      const next = this.stashed.get(i)
      if (next) {
        toReturn.push(next)
      } else {
        break
      }
    }

    return toReturn
  }
}

export enum Result {
  DUMPED = 'DUMPED', // store was dumped/replaced
  REVISED = 'REVISED', // store was revised
  TEMP = 'TEMP', // store was revised temporarily
  STASHED = 'STASHED', // attempted to revise store but sequence too high. revision stashed for later
  ERROR = 'ERROR', // attempted to revise/dump store, but failed
  NOOP = 'NOOP', // sequence too low, update ignored
}

// revise a collection of nodes.
export type Revision = { id: number, patch: Operation[], expireId: string | null }
// dump/replace the entire store with T
export type Dump<T> = { id: number, value: T, expireId: string | null }

export type Update<T> = UpdateReal<T> | UpdateTemp
export type UpdateReal<T> = Revision | Dump<T>
export type UpdateTemp = Omit<Revision, 'id' | 'expireId'> & { expiredBy : string }

function isTemp<T> (s: Update<T>): s is UpdateTemp {
  return !!(s as any).expiredBy
}

function isRevision<T> (s: Update<T>): s is Revision {
  return !isTemp(s) && !!(s as any).patch
}

function isDump<T> (s: UpdateReal<T>): s is Dump<T> {
  return !isTemp(s) && !!(s as any).value
}
