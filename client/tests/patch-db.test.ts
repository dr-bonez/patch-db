import { expect } from 'chai'
import { TestScheduler } from 'rxjs/testing'
import { PatchDB } from '../lib/patch-db'
import { MockSource } from './mocks/source.mock'
import { MockHttp } from './mocks/http.mock'
import { PatchOp } from '../lib/patch-db'
import { from } from 'rxjs'
import { MockBootstrapper } from './mocks/bootstrapper.mock'
import { UpdateReal } from '../lib/sequence-store'
import { RemoveOperation } from 'fast-json-patch'
import 'chai-string'

type Test = { a: string, b: number[], c: object, newKey?: string }

describe('patch db', function () {
  let scheduler: TestScheduler

  beforeEach(() => {
    scheduler = new TestScheduler((actual, expected) => {
      // console.log('actual', JSON.stringify(actual))
      // console.log('expected', JSON.stringify(expected))
      expect(actual).eql(expected)
    })
  })

  it('dumps', () => {
    scheduler.run(({ expectObservable, cold }) => {
      const initialData: Test = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const bootstrapper = new MockBootstrapper(0, initialData)
      const http = new MockHttp( { getSequences: [], getDump: { id: 0, value: { }, expireId: null } } )
      const updates = {
        a: { id: 1, value: { a: 'value1', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }, expireId: null },
        b: { id: 3, value: { a: 'value3', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }, expireId: null },
        c: { id: 2, value: { a: 'value2', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }, expireId: null }, // ooo for fun
      }
      const source = new MockSource<Test>(
        cold(Object.keys(updates).join(''), updates),
      )

      PatchDB.init({ sources: [source], http, bootstrapper }).then(pdb => {
        pdb.sync$().subscribe()
        expectObservable(pdb.store.watch$()).toBe('ab-', { a: updates.a.value, b: updates.b.value })
      })
    })
  })

  it('replaces + adds', () => {
    scheduler.run( ({ expectObservable, cold }) => {
      const initialData: Test = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
      const finalStore: Test = { a: 'value1', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 }, newKey: 'newValue' }
      const bootstrapper = new MockBootstrapper(0, initialData )
      const http = new MockHttp({ getSequences: [], getDump: { id: 0, value: { }, expireId: null } } )
      const updates = {
        a: { id: 1, value: { a: 'value1', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }, expireId: null },
        b: { id: 2, patch: [{ op: PatchOp.ADD, value: 'newValue', path: '/newKey' }], expireId: null},
      }
      const source = new MockSource<Test>(
        cold(Object.keys(updates).join(''), updates),
      )

      PatchDB.init({ sources: [source], http, bootstrapper }).then(pdb => {
        pdb.sync$().subscribe()
        expectObservable(pdb.store.watch$()).toBe('ab', { a: updates.a.value, b: finalStore })
      })
    })
  })

  it('gets db dump with invalid patch', done => {
    const initialData: Test = { a: 'value', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }
    const finalStore: Test = { a: 'value1', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 }, newKey: 'newValue' }
    const bootstrapper = new MockBootstrapper(0, initialData)
    const http = new MockHttp({ getSequences: [], getDump: { id: 2, value: finalStore, expireId: null } })
    const updates: UpdateReal<any>[] = [
      { id: 1, value: { a: 'value1', b: [1, 2, 3], c: { d: 1, e: 2, f: 3 } }, expireId: null },
      { id: 2, patch: [{ op: PatchOp.REMOVE, path: '/newKey' } as RemoveOperation], expireId: null},
    ]
    const source = new MockSource<Test>(
      from(updates),
    )

    PatchDB.init({ sources: [source], http, bootstrapper }).then(pdb => {
      let counter = 0
      pdb.store.watch$().subscribe(i => {
        counter ++
        if (counter === 2) done()
      })
      pdb.sync$().subscribe()
    })
  })
})
