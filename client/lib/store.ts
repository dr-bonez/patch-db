import { from, Observable, of } from 'rxjs'
import { toStream } from 'mobx-utils'
import { DBCache, Dump, Revision, Update } from './types'
import { applyPatch } from 'fast-json-patch'

export class Store<T extends { }> {
  cache: DBCache<T>

  constructor (
    readonly initialCache: DBCache<T>,
  ) {
    this.cache = initialCache
  }

  watch$ (): Observable<T>
  watch$<P1 extends keyof T> (p1: P1): Observable<T[P1]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1]> (p1: P1, p2: P2): Observable<T[P1][P2]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2]> (p1: P1, p2: P2, p3: P3): Observable<T[P1][P2][P3]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3]> (p1: P1, p2: P2, p3: P3, p4: P4): Observable<T[P1][P2][P3][P4]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3], P5 extends keyof T[P1][P2][P3][P4]> (p1: P1, p2: P2, p3: P3, p4: P4, p5: P5): Observable<T[P1][P2][P3][P4][P5]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3], P5 extends keyof T[P1][P2][P3][P4], P6 extends keyof T[P1][P2][P3][P4][P5]> (p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, p6: P6): Observable<T[P1][P2][P3][P4][P5][P6]>
  watch$ (...args: (string | number)[]): Observable<any> {
    return from(toStream(() => this.peekNode(...args), true))
  }

  watchAll$ (): Observable<DBCache<T>> {
    return of(this.cache)
  }

  update$ (update: Update<T>): Observable<DBCache<T>> {
    console.log('UPDATE:', update)
    if ((update as Revision).patch) {
      if (this.cache.sequence + 1 !== update.id) throw new Error(`Outdated sequence: current: ${this.cache.sequence}, new: ${update.id}`)
      applyPatch(this.cache.data, (update as Revision).patch, true, true)
    } else {
      this.cache.data = (update as Dump<T>).value
    }

    this.cache.sequence = update.id
    return of(this.cache)
  }

  reset (): void {
    this.cache = {
      sequence: 0,
      data: { },
    }
  }

  private peekNode (...args: (string | number)[]): any {
    try {
      return args.reduce((acc, next) => (acc as any)[`${next}`], this.cache.data)
    } catch (e) {
      return undefined
    }
  }
}
