import { Http } from '../../lib/patch-db'
import { Revision, Dump } from '../../lib/sequence-store'

export class MockHttp<T> implements Http<T> {
  constructor (private readonly mockData: {
    getSequences: Revision[],
    getDump: Dump<T>
  }) {  }

  getRevisions (): Promise<Revision[]> {
    return Promise.resolve(this.mockData.getSequences)
  }

  getDump (): Promise<Dump<T>> {
    return Promise.resolve(this.mockData.getDump)
  }
}
