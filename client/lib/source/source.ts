import { BehaviorSubject, Observable } from 'rxjs'
import { ConnectionStatus, Update } from '../types'

export interface Source<T> {
  connectionStatus$: BehaviorSubject<ConnectionStatus>
  watch$ (sequence$?: Observable<number>): Observable<Update<T>>
}
