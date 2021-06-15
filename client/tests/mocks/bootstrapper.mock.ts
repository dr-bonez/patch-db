import { Bootstrapper, DBCache } from '../../lib/patch-db'

export class MockBootstrapper<T> implements Bootstrapper<T> {

  constructor (
    private sequence: number = 0,
    private data: T = { } as T,
  ) { }

  async init (): Promise<DBCache<T>> {
    return {
      sequence: this.sequence,
      data: this.data as T,
    }
  }

  async update (cache: DBCache<T>): Promise<void> {
    this.sequence = cache.sequence
    this.data = cache.data
  }

  async clear (): Promise<void> {
    this.sequence = 0
    this.data = { } as T
  }
}