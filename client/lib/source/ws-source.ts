import { Observable } from 'rxjs'
import { webSocket, WebSocketSubject, WebSocketSubjectConfig } from 'rxjs/webSocket'
import { Update } from '../types'
import { Source } from './source'

export class WebsocketSource<T> implements Source<T> {
  private websocket$: WebSocketSubject<Update<T>>

  constructor (
    readonly url: string,
  ) {
    const fullConfig: WebSocketSubjectConfig<Update<T>> = {
      url,
      openObserver: {
        next: () => {
          console.log('WebSocket connection open')
          this.websocket$.next('open message' as any)
        },
      },
      closeObserver: {
        next: () => {
          console.log('WebSocket connection closed')
          // @TODO re-open websocket on retry loop
        },
      },
      closingObserver: {
        next: () => console.log('Websocket subscription cancelled, websocket closing'),
      },
    }
    this.websocket$ = webSocket(fullConfig)
  }

  watch$ (): Observable<Update<T>> { return this.websocket$.asObservable() }
}
