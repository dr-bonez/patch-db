import { merge, Observable, of } from 'rxjs'
import { concatMap, finalize, map, tap } from 'rxjs/operators'
import { Source } from './source/source'
import { Store } from './store'
import { DBCache } from './types'
export { Operation } from 'fast-json-patch'

export class PatchDB<T extends object> {
  store: Store<T>

  constructor (
    private readonly source: Source<T>,
    readonly cache: DBCache<T>,
  ) {
    this.store = new Store(cache)
  }

  sync$ (): Observable<DBCache<T>> {
    console.log('PATCHDB - sync$()')

    const sequence$ = this.store.watchAll$().pipe(map(cache => cache.sequence))
    // nested concatMaps, as it is written, ensure sync is not run for update2 until handleSyncResult is complete for update1.
    // flat concatMaps would allow many syncs to run while handleSyncResult was hanging. We can consider such an idea if performance requires it.
    return merge(this.source.watch$(sequence$)).pipe(
      tap(update => console.log('PATCHDB - source updated:', update)),
      concatMap(update => this.store.update$(update)),
      finalize(() => {
        console.log('PATCHDB - FINALIZING sync$()')
        this.store.reset()
      }),
    )
  }
}
