import { initializeApp, getApp, getApps } from 'firebase/app';
import { getFirestore } from 'firebase/firestore';

const firebaseConfig = {
	apiKey: 'AIzaSyCCbCaJcbZAV0silxwos8kEy1k-tpuKe3w',
	authDomain: 'vigilant-shell-435820-r2.firebaseapp.com',
	projectId: 'vigilant-shell-435820-r2',
	storageBucket: 'vigilant-shell-435820-r2.firebasestorage.app',
	messagingSenderId: '279923397259',
	appId: '1:279923397259:web:b1961c36dfe7abc605fa8e',
	measurementId: 'G-Z6V5P3347C'
};

// Initialize Firebase
const app = getApps().length ? getApp() : initializeApp(firebaseConfig);
export const db = getFirestore(app);
