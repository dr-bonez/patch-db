import { expect } from 'chai'
import { PatchOp } from '../lib/patch-db'
import { TestScheduler } from 'rxjs/testing'
import { Result, SequenceStore } from '../lib/sequence-store'
import { concatMap, map } from 'rxjs/operators'
import { Store } from '../lib/store'
import { RemoveOperation } from 'fast-json-patch'
import 'chai-string'

type TestStore = { a: string, b: number[], c?: { [key: string]: number } }
describe('sequence store temp functionality', function () {
  let scheduler: TestScheduler
  beforeEach(() => {
    scheduler = new TestScheduler( (actual, expected) => {
      // console.log('actual', JSON.stringify(actual))
      // console.log('expected', JSON.stringify(expected))
      expect(actual).eql(expected)
    })
  })

  it('applies a temp patch', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: TestStore = { a: 'value', b: [1, 2, 3], c: { g: 10 } }

    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    toTest.update$({
      patch: [{ op: PatchOp.REPLACE, value: finalStore.c, path: '/c' }],
      expiredBy: 'expireMe',
    }).subscribe(() => {
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(0)
      done()
    })
  })

  it('applies multiple temp patches', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: TestStore = { a: 'value', b: [0], c: { g: 10 } }

    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    const tempPatch1 = {
      patch: [{ op: PatchOp.REPLACE, value: finalStore.c, path: '/c' }],
      expiredBy: 'expireMe1',
    }

    const tempPatch2 = {
      patch: [{ op: PatchOp.REPLACE, value: finalStore.b, path: '/b' }],
      expiredBy: 'expireMe2',
    }

    toTest.update$(tempPatch1).pipe(concatMap(() =>
      toTest.update$(tempPatch2),
    )).subscribe(() => {
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(0)
      done()
    })
  })

  it('expires a temp patch', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const intermediaryStore: TestStore = { a: 'value', b: [1, 2, 3], c: { g: 10 } }
    const finalStore: TestStore = { a: 'value', b: [0], c: { d: 1, e: 2, f: 3 } }

    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    const tempPatch = {
      patch: [{ op: PatchOp.REPLACE, value: intermediaryStore.c, path: '/c' }],
      expiredBy: 'expireMe',
    }

    const expirePatch = {
      id: 1,
      patch: [{ op: PatchOp.REPLACE, value: finalStore.b, path: '/b' }],
      expireId: 'expireMe',
    }

    toTest.update$(tempPatch).pipe(
      map(() => expect(toTest.store.peek).eql(intermediaryStore)),
      concatMap(() => toTest.update$(expirePatch)),
    ).subscribe(() => {
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(1)
      done()
    })
  })

  it('expires a temp patch beneath a second temp patch', done => {
    const initialStore      : TestStore = { a: 'value' , b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const intermediaryStore : TestStore = { a: 'value' , b: [0]    , c: { g: 10 } }
    const finalStore        : TestStore = { a: 'valueX', b: [0]    , c: { d: 1, e: 2, f: 3 } }

    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    const tempPatch = {
      patch: [{ op: PatchOp.REPLACE, value: intermediaryStore.c, path: '/c' }],
      expiredBy: 'expireMe',
    }

    const tempPatch2 = {
      patch: [{ op: PatchOp.REPLACE, value: intermediaryStore.b, path: '/b' }],
      expiredBy: 'expireMe2',
    }

    const expirePatch = {
      id: 1,
      patch: [{ op: PatchOp.REPLACE, value: finalStore.a, path: '/a' }],
      expireId: 'expireMe',
    }

    toTest.update$(tempPatch).pipe(
      concatMap(() => toTest.update$(tempPatch2)),
      map(() => expect(toTest.store.peek).eql(intermediaryStore)),
      concatMap(() => toTest.update$(expirePatch)),
    ).subscribe(() => {
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(1)
      done()
    })
  })

  it('real patches are genuinely added beneath', done => {
    const initialStore      : TestStore = { a: 'value' , b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const intermediaryStore : TestStore = { a: 'value' , b: [1, 2, 3] }
    const finalStore        : TestStore = { a: 'value' , b: [1, 2, 3] }

    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    const tempPatch = {
      patch: [{ op: PatchOp.REMOVE, path: '/c' } as RemoveOperation],
      expiredBy: 'expireMe',
    }

    // this patch would error if the above had been a real patch and not a temp
    const realPatch = {
      id: 1,
      patch: [{ op: PatchOp.ADD, value: 4, path: '/c/g' }],
      expireId: null,
    }

    toTest.update$(tempPatch).pipe(
      map(res => expect(res).eql(Result.TEMP) && expect(toTest.store.peek).eql(intermediaryStore)),
      concatMap(() => toTest.update$(realPatch)),
    ).subscribe(res => {
      expect(res).eql(Result.REVISED)
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(1)
      done()
    })
  })
})
