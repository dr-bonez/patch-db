import { BehaviorSubject, from, Observable } from 'rxjs'
import { observable } from 'mobx'
import { toStream } from 'mobx-utils'
import { DBCache, Dump, Revision, Update } from './types'
import { applyPatch } from 'fast-json-patch'

export class Store<T> {
  sequence: number
  o: { data: T | { } }
  cache$: BehaviorSubject<DBCache<T>>

  constructor (
    readonly initialCache: DBCache<T>,
  ) {
    this.sequence = initialCache.sequence
    this.o = observable({ data: this.initialCache.data })
    this.cache$ = new BehaviorSubject(initialCache)
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

  watchCache$ (): Observable<DBCache<T>> {
    return this.cache$.asObservable()
  }

  update (update: Update<T>): void {
    if ((update as Revision).patch) {
      if (this.sequence + 1 !== update.id) throw new Error(`Outdated sequence: current: ${this.sequence}, new: ${update.id}`)
      applyPatch(this.o.data, (update as Revision).patch, true, true)
    } else {
      this.o.data = (update as Dump<T>).value
    }

    this.sequence = update.id

    this.cache$.next({ sequence: this.sequence, data: this.o.data })
  }

  reset (): void {
    this.cache$.next({
      sequence: 0,
      data: { },
    })
  }

  private peekNode (...args: (string | number)[]): any {
    try {
      return args.reduce((acc, next) => (acc as any)[`${next}`], this.o.data)
    } catch (e) {
      return undefined
    }
  }
}
