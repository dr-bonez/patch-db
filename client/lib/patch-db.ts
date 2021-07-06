import { merge, Observable } from 'rxjs'
import { concatMap, finalize, tap } from 'rxjs/operators'
import { Source } from './source/source'
import { Store } from './store'
import { DBCache } from './types'

export { Operation } from 'fast-json-patch'

export class PatchDB<T extends object> {
  store: Store<T>

  constructor (
    private readonly sources: Source<T>[],
    readonly cache: DBCache<T>,
  ) {
    this.store = new Store(cache)
  }

  sync$ (): Observable<DBCache<T>> {
    return merge(...this.sources.map(s => s.watch$(this.store)))
    .pipe(
      tap(update => this.store.update(update)),
      concatMap(() => this.store.watchCache$()),
      finalize(() => {
        this.store.reset()
      }),
    )
  }
}
