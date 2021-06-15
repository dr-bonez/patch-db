import { expect } from 'chai'
import { PatchOp } from '../lib/patch-db'
import { TestScheduler } from 'rxjs/testing'
import { Result, SequenceStore } from '../lib/sequence-store'
import { concatMap, map } from 'rxjs/operators'
import { Store } from '../lib/store'
import { RemoveOperation } from 'fast-json-patch'
import 'chai-string'

type TestStore = { a: string, b: number[], c?: { [key: string]: number } }
describe('sequence store', function () {
  let scheduler: TestScheduler
  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) => {
      // console.log('actual', JSON.stringify(actual))
      // console.log('expected', JSON.stringify(expected))
      expect(actual).eql(expected)
    })
  })

  it('dumps', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: TestStore = { a: 'valueX', b: [0], c: { g: 10 } }
    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    toTest.update$({ id: 5, value: finalStore, expireId: null }).subscribe(() => {
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(5)
      done()
    })
  })

  it('ignores dump for id too low', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: TestStore = { a: 'valueX', b: [0], c: { g: 10 } }
    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    toTest.update$({ id: 5, value: finalStore, expireId: null }).pipe(concatMap(() =>
      toTest.update$({ id: 4, value: initialStore, expireId: null }),
    )).subscribe(() => {
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(5)
      done()
    })
  })

  it('revises', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: TestStore = { a: 'value', b: [1, 2, 3], c: { g: 10 } }
    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    toTest.update$({ id: 1, patch: [{ op: PatchOp.REPLACE, value: finalStore.c, path: '/c' }], expireId: null }).subscribe(() => {
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(1)
      done()
    })
  })

  it('saves a revision when not next in line', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: TestStore = { a: 'value', b: [1, 2, 3], c: { g: 10 } }
    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    toTest.update$({ id: 2, patch: [{ op: PatchOp.REPLACE, value: finalStore.c, path: '/c' }], expireId: null }).subscribe(() => {
      expect(toTest.store.peek).eql(initialStore)
      expect(toTest.sequence).eql(0)
      done()
    })
  })

  it('applies saved revisions when contiguous revisions become available', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: TestStore = { a: 'value', b: [1, 2, 3, 4], c: { g: 10 } }
    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    toTest.update$({ id: 2, patch: [{ op: PatchOp.REPLACE, value: finalStore.c, path: '/c' }], expireId: null }).pipe(concatMap(() =>
      toTest.update$({ id: 1, patch: [{ op: PatchOp.ADD, value: 4, path: '/b/-' }], expireId: null }),
    )).subscribe(() => {
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(2)
      done()
    })
  })

  it('applies saved revisions when contiguous revisions become available part 2', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: TestStore = { a: 'value2', b: [0], c: { g: 10 } }
    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    toTest.update$({ id: 2, patch: [{ op: PatchOp.REPLACE, value: finalStore.c, path: '/c' }], expireId: null }).pipe(concatMap(() =>
      toTest.update$({ id: 1, value: { a: 'value2', b: [0], c: { d: 1, e: 2, f: 3 } }, expireId: null }),
    )).subscribe(() => {
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(2)
      done()
    })
  })

  it('wipes out stashed patches when sequence is force updated', done => {
    const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: TestStore = { a: 'value2', b: [0], c: { g: 10 } }
    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    // patch gets stashed
    expect(toTest.viewRevisions().length).eql(0)

    toTest.update$({ id: 2, patch: [{ op: PatchOp.REPLACE, value: finalStore.c, path: '/c' }], expireId: null }).pipe(
      map(res => expect(res).eql(Result.STASHED) && expect(toTest.viewRevisions().length).eql(1)),
      concatMap(() => toTest.update$({ id: 3, value: finalStore, expireId: null })),
      map(res => expect(res).eql(Result.DUMPED) && expect(toTest.viewRevisions().length).eql(0)),
    ).subscribe(() => done())
  })

  it('emits sequence + state on updates (revisions)', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const finalStore: TestStore = { a: 'value2', b: [0], c: { g: 10 } }

      const store = new Store(initialStore)
      const toTest = new SequenceStore(store, 0)
      const expectedStream = 'ab'

      cold('-b').subscribe(() => {
        toTest.update$({ id: 3, value: finalStore, expireId: null }).subscribe()
      })

      expectObservable(toTest.watch$().pipe(
        map(cache => ({ sequence: cache.sequence, contents: cache.data})),
      )).toBe(expectedStream, {
        a: { sequence: 0, contents: initialStore },
        b: { sequence: 3, contents: finalStore },
      })
    })
  })

  it('emits sequence + state on updates (patch)', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const initialStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const finalStore: TestStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3, g: 4 } }

      const store = new Store(initialStore)
      const toTest = new SequenceStore(store, 0)
      const expectedStream = 'ab'

      cold('-b').subscribe(() => {
        toTest.update$({ id: 1, patch: [{ op: PatchOp.ADD, path: '/c/g', value: 4 }], expireId: null }).subscribe()
      })

      expectObservable(toTest.watch$().pipe(
        map(cache => ({ sequence: cache.sequence, contents: cache.data })),
      )).toBe(expectedStream, {
        a: { sequence: 0, contents: initialStore },
        b: { sequence: 1, contents: finalStore },
      })
    })
  })

  it('errors bubble out in results', done => {
    const initialStore      : TestStore = { a: 'value' , b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const intermediaryStore : TestStore = { a: 'value' , b: [1, 2, 3] }
    const finalStore        : TestStore = { a: 'value' , b: [1, 2, 3] }

    const store = new Store(initialStore)
    const toTest = new SequenceStore(store, 0)

    const patch1 = {
      id: 1,
      patch: [{ op: PatchOp.REMOVE, path: '/c' } as RemoveOperation],
      expireId: null,
    }

    const patch2 = {
      id: 2,
      patch: [{ op: PatchOp.ADD, value: 4, path: '/c/g' }],
      expireId: null,
    }

    toTest.update$(patch1).pipe(
      map(res => expect(res).eql(Result.REVISED) && expect(toTest.store.peek).eql(intermediaryStore)),
      concatMap(() => toTest.update$(patch2)),
    ).subscribe(res => {
      expect(res).eql(Result.ERROR)
      expect(toTest.store.peek).eql(finalStore)
      expect(toTest.sequence).eql(1)
      done()
    })
  })
})
