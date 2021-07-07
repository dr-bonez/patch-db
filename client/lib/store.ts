import { DBCache, Dump, Revision, Update } from './types'
import { applyPatch, getValueByPointer } from 'fast-json-patch'
import { BehaviorSubject, Observable } from 'rxjs'
import { finalize } from 'rxjs/operators'

export class Store<T> {
  cache: DBCache<T>
  sequence$: BehaviorSubject<number>
  private nodes: { [path: string]: BehaviorSubject<any> } = { }

  constructor (
    readonly initialCache: DBCache<T>,
  ) {
    this.cache = initialCache
    this.sequence$ = new BehaviorSubject(initialCache.sequence)
  }

  watch$ (): Observable<T>
  watch$<P1 extends keyof T> (p1: P1): Observable<T[P1]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1]> (p1: P1, p2: P2): Observable<T[P1][P2]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2]> (p1: P1, p2: P2, p3: P3): Observable<T[P1][P2][P3]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3]> (p1: P1, p2: P2, p3: P3, p4: P4): Observable<T[P1][P2][P3][P4]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3], P5 extends keyof T[P1][P2][P3][P4]> (p1: P1, p2: P2, p3: P3, p4: P4, p5: P5): Observable<T[P1][P2][P3][P4][P5]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3], P5 extends keyof T[P1][P2][P3][P4], P6 extends keyof T[P1][P2][P3][P4][P5]> (p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, p6: P6): Observable<T[P1][P2][P3][P4][P5][P6]>
  watch$ (...args: (string | number)[]): Observable<any> {
    const path = `/${args.join('/')}`
    if (!this.nodes[path]) {
      this.nodes[path] = new BehaviorSubject(getValueByPointer(this.cache.data, path))
      this.nodes[path].pipe(
        finalize(() => delete this.nodes[path]),
      )
    }
    return this.nodes[path].asObservable()
  }

  update (update: Update<T>): void {
    if ((update as Revision).patch) {
      if (this.cache.sequence + 1 !== update.id) throw new Error(`Outdated sequence: current: ${this.cache.sequence}, new: ${update.id}`)
      applyPatch(this.cache.data, (update as Revision).patch, true, true);
      (update as Revision).patch.forEach(op => {
        this.updateNodesByPath(op.path)
      })

    } else {
      this.cache.data = (update as Dump<T>).value
      this.updateNodesByPath('')
    }
    this.cache.sequence = update.id
    this.sequence$.next(this.cache.sequence)
  }

  updateNodesByPath (revisionPath: string) {
    Object.keys(this.nodes).forEach(nodePath => {
      if (!this.nodes[nodePath]) return
      if (nodePath.includes(revisionPath) || revisionPath.includes(nodePath)) {
        try {
          this.nodes[nodePath].next(getValueByPointer(this.cache.data, nodePath))
        } catch (e) {
          this.nodes[nodePath].complete()
          delete this.nodes[nodePath]
        }
      }
    })
  }

  reset (): void {
    Object.values(this.nodes).forEach(node => node.complete())
  }
}
