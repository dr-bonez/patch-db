import { Observable } from 'rxjs'
import { UpdateReal } from '../../lib/sequence-store'
import { Source } from '../../lib/source/source'

export class MockSource<T> implements Source<T> {

  constructor (
    private readonly mockData: Observable<UpdateReal<T>>,
  ) { }

  watch$ (): Observable<UpdateReal<T>> {
    return this.mockData
  }

  start (): void {  }

  stop (): void {  }
}
