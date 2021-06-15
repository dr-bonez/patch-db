import { from, Observable } from 'rxjs'
import { applyPatch, Operation } from 'fast-json-patch'
import { observable, runInAction } from 'mobx'
import { toStream } from 'mobx-utils'

export class Store<T extends object> {
  private o: { data: T }

  constructor (data: T) {
    this.o = observable({ data })
  }

  get peek (): T { return this.o.data }

  watch$ (): Observable<T>
  watch$<P1 extends keyof T> (p1: P1): Observable<T[P1]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1]> (p1: P1, p2: P2): Observable<T[P1][P2]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2]> (p1: P1, p2: P2, p3: P3): Observable<T[P1][P2][P3]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3]> (p1: P1, p2: P2, p3: P3, p4: P4): Observable<T[P1][P2][P3][P4]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3], P5 extends keyof T[P1][P2][P3][P4]> (p1: P1, p2: P2, p3: P3, p4: P4, p5: P5): Observable<T[P1][P2][P3][P4][P5]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3], P5 extends keyof T[P1][P2][P3][P4], P6 extends keyof T[P1][P2][P3][P4][P5]> (p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, p6: P6): Observable<T[P1][P2][P3][P4][P5][P6]>
  watch$ (...args: (string | number)[]): Observable<any> {
    return from(toStream(() => this.peekAccess(...args), true))
  }

  set (data: T): void {
    runInAction(() => this.o.data = data)
  }

  applyPatchDocument (patch: Operation[]): { oldDocument: T, newDocument: T } {
    const oldDocument = this.o.data
    const newDocument = patchDocument(patch, oldDocument)
    this.set(newDocument)
    return { oldDocument, newDocument }
  }

  private peekAccess (...args: (string | number)[]): any {
    try {
      return args.reduce((acc, next) => (acc as any)[`${next}`], this.o.data)
    } catch (e) {
      return undefined
    }
  }
}

export function patchDocument<T> (patch: Operation[], doc: T): T {
  return applyPatch(doc, patch, true, false).newDocument
}
