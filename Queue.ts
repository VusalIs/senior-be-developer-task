import { Message } from "./Database";

export class Queue {
  // Per-key FIFO buckets of pending messages. Ensures operations for the same key keep their order.
  private messagesMap = new Map<string, Message[]>();

  // Keys that currently have at least one pending message
  private availableKeys = new Set<string>();

  // Ownership of a key: which worker (by id) is currently allowed to dequeue/process this key.
  private keyOwner = new Map<string, number>();

  // Reverse lookup: for a given worker id, which key it currently owns (if any).
  private workerKey = new Map<number, string>();

  // Messages that have been given to a worker but not yet confirmed: per worker -> messageId -> { key, msg }.
  private inFlight = new Map<
    number,
    Map<string, { key: string; msg: Message }>
  >();

  Enqueue = (message: Message) => {
    const bucket = this.ensureBucket(message.key);
    
    bucket.push(message);

    this.availableKeys.add(message.key);
  };

  Dequeue = (workerId: number): Message | undefined => {
    const ownedKey = this.workerKey.get(workerId);

    if (ownedKey) {
      const messagesByKey = this.messagesMap.get(ownedKey)!;

      if (messagesByKey.length === 0) {
        return undefined;
      }

      const msg = messagesByKey.shift()!;

      this.trackInFlight(workerId, ownedKey, msg);

      return msg;
    }

    const key = this.popAvailableKey();

    if (!key) return undefined;

    this.keyOwner.set(key, workerId);
    this.workerKey.set(workerId, key);

    const bucket = this.messagesMap.get(key)!;
    const msg = bucket.shift()!;

    this.trackInFlight(workerId, key, msg);

    return msg;
  };

  Confirm = (workerId: number, messageId: string): void => {
    const map = this.inFlight.get(workerId);

    if (!map) return;

    const entry = map.get(messageId);

    if (!entry) return;

    map.delete(messageId);

    if (map.size === 0) this.inFlight.delete(workerId);

    const key = entry.key;
    const bucket = this.messagesMap.get(key)!;

    if (bucket.length === 0) {
      this.keyOwner.delete(key);

      if (this.workerKey.get(workerId) === key) {
        this.workerKey.delete(workerId);
      }

      if (bucket.length > 0) {
        this.availableKeys.add(key);
      }
    }
  };

  Size = (): number => {
    let pending = 0;

    for (const arr of this.messagesMap.values()) pending += arr.length;

    let inflight = 0;

    for (const m of this.inFlight.values()) inflight += m.size;

    return pending + inflight;
  };

  private ensureBucket(key: string): Message[] {
    if (!this.messagesMap.has(key)) this.messagesMap.set(key, []);

    return this.messagesMap.get(key)!;
  }

  private popAvailableKey(): string | undefined {
    for (const key of this.availableKeys) {
      const bucket = this.messagesMap.get(key);

      if (bucket && bucket.length > 0 && !this.keyOwner.has(key)) {
        this.availableKeys.delete(key);

        return key;
      }

      this.availableKeys.delete(key);
    }

    return undefined;
  }

  private trackInFlight(workerId: number, key: string, msg: Message) {
    if (!this.inFlight.has(workerId)) this.inFlight.set(workerId, new Map());

    this.inFlight.get(workerId)!.set(msg.id, { key, msg });
  }
}
