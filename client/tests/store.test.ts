import { expect } from 'chai'
import { PatchOp } from '../lib/patch-db'
import { TestScheduler } from 'rxjs/testing'
import { Store } from '../lib/store'
import { tap } from 'rxjs/operators'
import { AddOperation, RemoveOperation, ReplaceOperation } from 'fast-json-patch'
import 'chai-string'

describe('rx store', function () {
  let scheduler: TestScheduler
  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) => {
      // console.log('actual', JSON.stringify(actual))
      // console.log('expected', JSON.stringify(expected))
      expect(actual).eql(expected)
    })
  })

  it('returns old and new store state', () => {
    const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const expectedFinalStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 }, newKey: 'newValue', newKey2: 'newValue2', newKey3: 'newValue3' }
    const toTest = new Store(initialStore)
    const add: AddOperation<string> = { op: PatchOp.ADD, value: 'newValue', path: '/newKey' }
    const add2: AddOperation<string> = { op: PatchOp.ADD, value: 'newValue2', path: '/newKey2' }
    const add3: AddOperation<string> = { op: PatchOp.ADD, value: 'newValue3', path: '/newKey3' }

    const { oldDocument, newDocument} = toTest.applyPatchDocument([add, add2, add3])
    expect(oldDocument).eql(initialStore)
    expect(newDocument).eql(expectedFinalStore)
  })

  it('adds', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const expectedIntermediateStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 }, newKey: 'newValue', newKey2: 'newValue2' }
      const expectedFinalStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 }, newKey: 'newValue', newKey2: 'newValue2', newKey3: 'newValue3' }
      const toTest = new Store(initialStore)
      const add: AddOperation<string> = { op: PatchOp.ADD, value: 'newValue', path: '/newKey' }
      const add2: AddOperation<string> = { op: PatchOp.ADD, value: 'newValue2', path: '/newKey2' }
      const add3: AddOperation<string> = { op: PatchOp.ADD, value: 'newValue3', path: '/newKey3' }
      const expectedStream = 'abc'

      cold('-bc', { b: [add, add2], c: [add3] }).subscribe(i => toTest.applyPatchDocument(i))
      expectObservable(toTest.watch$()).toBe(expectedStream, { a: initialStore, b: expectedIntermediateStore, c: expectedFinalStore })
    })
  })

  it('adds + revises + removes', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const expectedFinalStore = { a: 'value', b: [1, 2, 3], newKey: 'newValue', newKey2: 'newValue3' }
      const toTest = new Store(initialStore)
      const add: AddOperation<string> = { op: PatchOp.ADD, value: 'newValue', path: '/newKey' }
      const add2: AddOperation<string> = { op: PatchOp.ADD, value: 'newValue2', path: '/newKey2' }
      const revise: ReplaceOperation<string> = { op: PatchOp.REPLACE, value: 'newValue3', path: '/newKey2' }
      const remove: RemoveOperation = { op: PatchOp.REMOVE, path: '/c' }
      const expectedStream = 'ab'

      cold('-b').subscribe(_ => toTest.applyPatchDocument([add, add2, revise, remove]))
      expectObservable(toTest.watch$()).toBe(expectedStream, { a: initialStore, b: expectedFinalStore })
    })
  })

  it('serializes', done => {
    const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const intermediaryStore = { a: 'value', b: [1, 2, 3], newKey: 'newValue', c: { d: 1, e: 2, f: 3 } }
    const toTest = new Store(initialStore)

    const add: AddOperation<string> = { op: PatchOp.ADD, value: 'newValue', path: '/newKey' }
    const unAdd: RemoveOperation = { op: PatchOp.REMOVE, path: '/newKey' }

    let i = 0
    toTest.watch$().subscribe(t => {
      if (i === 0) { expect(t).eql(initialStore) }
      if (i === 1) { expect(t).eql(intermediaryStore) }
      if (i === 2) { expect(t).eql(initialStore); done() }
      i += 1
    })
    toTest.applyPatchDocument([add])
    toTest.applyPatchDocument([unAdd])
  })

  it('doesnt apply invalid patches', done => {
    const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const toTest = new Store(initialStore)

    const removeValid: RemoveOperation = { op: PatchOp.REMOVE, path: '/b' }
    const removeInvalid: RemoveOperation = { op: PatchOp.REMOVE, path: '/newKey' }
    try {
      toTest.applyPatchDocument([removeValid, removeInvalid])
      expect(true).eql('We expected an error here')
    } catch (e) {
      toTest.watch$().subscribe(t => {
        expect(t).eql(initialStore)
        done()
      })
    }
  })

  it('emits undefined when key disappears', done => {
    const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const store = new Store(initialStore)

    const remove: RemoveOperation = { op: PatchOp.REMOVE, path: '/c' }
    let counter = 0
    store.watch$('c', 'd').pipe(tap(() => counter++)).subscribe({
      next: i => {
        if (counter === 1) expect(i).eql(initialStore.c.d)
        if (counter === 2) expect(i).eql(undefined)
        if (counter === 2) done()
      },
    })
    store.applyPatchDocument([remove])
  })

  it('when key returns, sub continues', done => {
    const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const store = new Store(initialStore)

    const remove: RemoveOperation = { op: PatchOp.REMOVE, path: '/c' }
    const reAdd: AddOperation<{ d: number }> = { op: PatchOp.ADD, path: '/c', value: { d: 1 } }
    let counter = 0
    store.watch$('c', 'd').pipe(tap(() => counter++)).subscribe({
      next: i => {
        if (counter === 1) expect(i).eql(initialStore.c.d)
        if (counter === 2) expect(i).eql(undefined)
        if (counter === 3) expect(i).eql(reAdd.value.d)
        if (counter === 3) done()
      },
    })
    store.applyPatchDocument([remove])
    store.applyPatchDocument([reAdd])
  })

  it('watches a single property', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const store = new Store(initialStore)
      const toTest = store.watch$('b', 1)

      const revise: ReplaceOperation<number> = { op: PatchOp.REPLACE, value: 4, path: '/b/1' }

      const expectedStream = 'ab'

      cold('-b').subscribe(_ => store.applyPatchDocument([revise]))
      expectObservable(toTest).toBe(expectedStream, { a: initialStore.b[1], b: revise.value })
    })
  })

  it('property only emits if it is updated', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const store = new Store(initialStore)
      const toTest = store.watch$('b', 0)

      const revise: ReplaceOperation<number> = { op: PatchOp.REPLACE, value: 4, path: '/b/1' }

      const expectedStream = 'a-'

      cold('-b').subscribe(_ => store.applyPatchDocument([revise]))
      expectObservable(toTest).toBe(expectedStream, { a: initialStore.b[0] })
    })
  })

  it('only does the last updates', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const store = new Store(initialStore)
      const toTest = store.watch$('b', 1)

      const revise1: ReplaceOperation<number> = { op: PatchOp.REPLACE, value: 4, path: '/b/1' }
      const revise2: ReplaceOperation<number> = { op: PatchOp.REPLACE, value: 5, path: '/b/1' }

      const expectedStream = 'ab'

      cold('-b').subscribe(_ => store.applyPatchDocument([revise1, revise2]))
      expectObservable(toTest).toBe(expectedStream, { a: initialStore.b[1], b: revise2.value })
    })
  })

  it('emits multiple updates', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const initialStore = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const store = new Store(initialStore)
      const toTest = store.watch$('b', 1)

      const revise1: ReplaceOperation<number> = { op: PatchOp.REPLACE, value: 4, path: '/b/1' }
      const revise2: ReplaceOperation<number> = { op: PatchOp.REPLACE, value: 5, path: '/b/1' }

      const expectedStream = 'abc'

      cold('-bc', { b: revise1, c: revise2 }).subscribe(i => {
        store.applyPatchDocument([i])
      })
      expectObservable(toTest).toBe(expectedStream, { a: initialStore.b[1], b: revise1.value, c: revise2.value})
    })
  })

  it('does a BIG store', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const fatty = require('./mocks/mock-data.json')
      const store = new Store(fatty)
      const toTest = store.watch$('kind')

      const revise: ReplaceOperation<string> = { op: PatchOp.REPLACE, value: 'testing', path: '/kind' }
      const expectedStream = 'ab'

      cold('-b', { b: revise }).subscribe(i => {
        store.applyPatchDocument([i])
      })
      expectObservable(toTest).toBe(expectedStream, { a: fatty['kind'], b: revise.value })
    })
  })
})
