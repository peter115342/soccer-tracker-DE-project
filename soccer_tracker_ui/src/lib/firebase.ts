import { initializeApp, getApp, getApps } from 'firebase/app';
import { getFirestore } from 'firebase/firestore';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';

const getFirebaseConfig = async () => {
	const client = new SecretManagerServiceClient();
	const [version] = await client.accessSecretVersion({
		name: 'projects/vigilant-shell-435820-r2/secrets/firebase-config/versions/latest'
	});

	if (!version.payload || !version.payload.data) {
		throw new Error('Failed to load Firebase configuration from Secret Manager');
	}

	return JSON.parse(version.payload.data.toString());
};

const initializeFirebase = async () => {
	const firebaseConfig = await getFirebaseConfig();
	const app = getApps().length ? getApp() : initializeApp(firebaseConfig);
	return getFirestore(app);
};

export const db = await initializeFirebase();
