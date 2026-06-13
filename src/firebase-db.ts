export function isFirebaseEnabled(): boolean {
  return false;
}

export function getFirestoreDb() {
  return null;
}

export enum OperationType {
  CREATE = 'create',
  UPDATE = 'update',
  DELETE = 'delete',
  LIST = 'list',
  GET = 'get',
  WRITE = 'write',
}

export async function syncFromFirestore(): Promise<any> {
  return null;
}

export async function syncToFirestore(data: any): Promise<void> {
  // Unconditionally severed connection to Firebase as requested
  return;
}

