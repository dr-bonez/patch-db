import { Observable } from 'rxjs'
import { webSocket, WebSocketSubject, WebSocketSubjectConfig } from 'rxjs/webSocket'
import { UpdateReal } from '../sequence-store'
import { Source } from './source'

export class WebsocketSource<T> implements Source<T> {
  private websocket$: WebSocketSubject<UpdateReal<T>>

  constructor (
    readonly url: string,
  ) {
    const fullConfig: WebSocketSubjectConfig<UpdateReal<T>> = {
      url,
      openObserver: {
        next: () => console.log('WebSocket connection open'),
      },
      closeObserver: {
        next: () => console.log('WebSocket connection closed'),
      },
      closingObserver: {
        next: () => console.log('Websocket subscription cancelled, websocket closing'),
      },
    }
    this.websocket$ = webSocket(fullConfig)
  }

  watch$ (): Observable<UpdateReal<T>> { return this.websocket$.asObservable() }
}
