import { Observable } from 'rxjs'
import { Update } from '../sequence-store'

export interface Source<T> {
  watch$ (sequence$?: Observable<number>): Observable<Update<T>>
}
