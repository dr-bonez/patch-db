import { BehaviorSubject, concat, from, Observable, of } from 'rxjs'
import { catchError, concatMap, delay, skip, switchMap, take, tap } from 'rxjs/operators'
import { ConnectionStatus, Http, Update } from '../types'
import { Source } from './source'

export type PollConfig = {
  cooldown: number
}

export class PollSource<T> implements Source<T> {
  connectionStatus$ = new BehaviorSubject(ConnectionStatus.Initializing)

  constructor (
    private readonly pollConfig: PollConfig,
    private readonly http: Http<T>,
  ) { }

  watch$ (sequence$: Observable<number>): Observable<Update<T>> {
    const polling$ = new BehaviorSubject('')

    const updates$ = of('').pipe(
      concatMap(_ => sequence$),
      take(1),
      concatMap(seq => this.http.getRevisions(seq)),
      tap(_ => this.connectionStatus$.next(ConnectionStatus.Connected)),
      catchError(e => {
        console.error(e)
        this.connectionStatus$.next(ConnectionStatus.Disconnected)
        return of([])
      }),
    )

    const delay$ = of([]).pipe(
      delay(this.pollConfig.cooldown),
      tap(_ => polling$.next('')),
      skip(1),
    )

    const poll$ = concat(updates$, delay$)

    return polling$.pipe(
       switchMap(_ => poll$),
       concatMap(res => {
        if (Array.isArray(res)) {
          return from(res) // takes Revision[] and converts it into Observable<Revision>
        } else {
          return of(res) // takes Dump<T> and converts it into Observable<Dump<T>>
        }
       }),
    )
  }
}
