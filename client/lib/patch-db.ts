import { merge, Observable, of } from 'rxjs'
import { concatMap, finalize, tap } from 'rxjs/operators'
import { Source } from './source/source'
import { Store } from './store'
import { DBCache } from './types'

export { Operation } from 'fast-json-patch'

export class PatchDB<T> {
  store: Store<T>

  constructor (
    private readonly sources: Source<T>[],
    private readonly initialCache: DBCache<T>,
  ) {
    this.store = new Store(this.initialCache)
  }

  sync$ (): Observable<DBCache<T>> {
    return merge(...this.sources.map(s => s.watch$(this.store)))
    .pipe(
      tap(update => this.store.update(update)),
      concatMap(() => of(this.store.cache)),
      finalize(() => {
        this.store.reset()
      }),
    )
  }
}
