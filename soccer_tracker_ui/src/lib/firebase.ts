import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { initializeApp, getApp, getApps } from 'firebase/app';
import { getFirestore } from 'firebase/firestore';

let dbInstance: ReturnType<typeof getFirestore> | null = null;

async function getFirebaseConfig() {
	const client = new SecretManagerServiceClient();

	const [version] = await client.accessSecretVersion({
		name: 'projects/vigilant-shell-435820-r2/secrets/firebase-config/versions/latest'
	});

	const payload = version.payload?.data?.toString();
	return JSON.parse(payload || '{}');
}

export async function getDb() {
	if (!dbInstance) {
		const firebaseConfig = await getFirebaseConfig();
		const app = getApps().length ? getApp() : initializeApp(firebaseConfig);
		dbInstance = getFirestore(app);
	}
	return dbInstance;
}
