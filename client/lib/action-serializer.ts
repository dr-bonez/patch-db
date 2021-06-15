import { Subject, BehaviorSubject, of, Observable, Observer, throwError } from 'rxjs'
import { concatMap, map, catchError, filter, take } from 'rxjs/operators'

export type Action<T> = {
  action: () => T,
  notify: BehaviorSubject<undefined | T>
}

export class ActionSerializer {
  private readonly sequentialActions = new Subject<Action<any>>()

  constructor () {
    this.sequentialActions.pipe(
      concatMap(({ action, notify }) => fromSync$(action).pipe(
        catchError(e => of(notify.next({ error: e }))),
        map(result => notify.next({ result })),
      )),
      catchError(e => of(console.error(`Action Serializer Exception`, e))),
    ).subscribe()
  }

  run$<T> (action: () => T): Observable<T> {
    const notify = new BehaviorSubject(undefined) as BehaviorSubject<T | undefined>
    this.sequentialActions.next({ action, notify })
    return (notify as BehaviorSubject<T>).pipe(
      filter(res => res !== undefined),
      take(1),
      concatMap((res: any) => res.error ? throwError(res.error) : of(res.result)),
    )
  }
}

function fromSync$<S, T> (action: (s: S) => T, s: S): Observable<T>
function fromSync$<T> (action: () => T): Observable<T>
function fromSync$<S, T> (action: (s: S) => T, s?: S): Observable<T> {
  return new Observable((subscriber: Observer<T>) => {
    try {
      subscriber.next(action(s as S))
      subscriber.complete()
    } catch (e) {
      subscriber.error(e)
    }
  })
}
