import { BehaviorSubject, Observable } from 'rxjs'
import { webSocket, WebSocketSubject, WebSocketSubjectConfig } from 'rxjs/webSocket'
import { ConnectionStatus, Update } from '../types'
import { Source } from './source'

export class WebsocketSource<T> implements Source<T> {
  connectionStatus$ = new BehaviorSubject(ConnectionStatus.Initializing)
  private websocket$: WebSocketSubject<Update<T>> | undefined

  constructor (
    private readonly url: string,
  ) {
  }

  watch$ (): Observable<Update<T>> {
    const fullConfig: WebSocketSubjectConfig<Update<T>> = {
      url: this.url,
      openObserver: {
        next: () => {
          console.log('WebSocket connection open')
          this.connectionStatus$.next(ConnectionStatus.Connected)
          this.websocket$!.next('open message' as any)
        },
      },
      closeObserver: {
        next: () => {
          this.connectionStatus$.next(ConnectionStatus.Disconnected)
          console.log('WebSocket connection closed')
        },
      },
      closingObserver: {
        next: () => {
          console.log('Websocket subscription cancelled, websocket closing')
        },
      },
    }
    this.websocket$ = webSocket(fullConfig)
    return this.websocket$
  }
}
