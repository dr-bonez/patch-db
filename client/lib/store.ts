import { DBCache, Dump, Http, Revision, Update } from './types'
import { applyPatch, getValueByPointer } from 'fast-json-patch'
import { BehaviorSubject, Observable } from 'rxjs'
import { finalize } from 'rxjs/operators'
import BTree from 'sorted-btree'

export class Store<T> {
  cache: DBCache<T>
  sequence$: BehaviorSubject<number>
  private nodes: { [path: string]: BehaviorSubject<any> } = { }
  private stashed = new BTree<number, Revision>()

  constructor (
    private readonly http: Http<T>,
    private readonly initialCache: DBCache<T>,
  ) {
    this.cache = this.initialCache
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
    // if stale, return
    if (update.id <= this.cache.sequence) return

    if (this.isRevision(update)) {
      this.handleRevision(update)
    } else {
      this.handleDump(update)
    }
  }

  reset (): void {
    Object.values(this.nodes).forEach(node => node.complete())
    this.stashed.clear()
  }

  private handleRevision (revision: Revision): void {
    // stash the revision
    this.stashed.set(revision.id, revision)
    // if revision is futuristic, fetch missing revisions and return
    if (revision.id > this.cache.sequence + 1) {
      this.http.getRevisions(this.cache.sequence)
      return
    // if revision is next in line, apply contiguous stashed
    } else {
      this.processStashed(revision.id)
    }
  }

  private handleDump (dump: Dump<T>): void {
    this.cache.data = dump.value
    this.stashed.deleteRange(this.cache.sequence, dump.id, false)
    this.updateNodesByPath('')
    this.updateSequence(dump.id)
    this.processStashed(dump.id + 1)
  }

  private processStashed (id: number): void {
    while (true) {
      const revision = this.stashed.get(id)
      if (!revision) break
      applyPatch(this.cache.data, revision.patch, true, true)
      revision.patch.map(op => {
        this.updateNodesByPath(op.path)
      })
      this.updateSequence(id)
      id++
    }
    this.stashed.deleteRange(0, id, false)
  }

  private updateNodesByPath (revisionPath: string) {
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

  private updateSequence (sequence: number): void {
    this.cache.sequence = sequence
    this.sequence$.next(sequence)
  }

  private isRevision (update: Update<T>): update is Revision {
    return !!(update as Revision).patch
  }
}
