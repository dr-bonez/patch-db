import { Observable } from 'rxjs'
import { Update } from '../types'

export interface Source<T> {
  watch$ (sequence$?: Observable<number>): Observable<Update<T>>
}
