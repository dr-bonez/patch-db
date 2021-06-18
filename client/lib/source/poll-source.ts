import { BehaviorSubject, concat, from, Observable, of } from 'rxjs'
import { catchError, concatMap, delay, skip, switchMap, take, tap } from 'rxjs/operators'
import { Http, Update } from '../types'
import { Source } from './source'

export type PollConfig = {
  cooldown: number
}

export class PollSource<T> implements Source<T> {

  constructor (
    private readonly pollConfig: PollConfig,
    private readonly http: Http<T>,
  ) { }

  watch$ (sequence$: Observable<number>): Observable<Update<T>> {
    console.log('POLL_SOURCE - watch$()')

    const polling$ = new BehaviorSubject('')

    const updates$ = of('').pipe(
      concatMap(_ => sequence$),
      take(1),
      tap(_ => console.log('making request')),
      concatMap(seq => this.http.getRevisions(seq)),
      catchError(e => {
        console.error(e)
        return of([])
      }),
      tap(_ => console.log('request complete')),
    )

    const delay$ = of([]).pipe(
      tap(_ => console.log('starting cooldown')),
      delay(this.pollConfig.cooldown),
      tap(_ => console.log('cooldown finished')),
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
