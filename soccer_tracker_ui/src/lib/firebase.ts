import { initializeApp } from 'firebase/app';
import { getFirestore } from 'firebase/firestore';

const firebaseConfig = {
    projectId: "vigilant-shell-435820",
};

const app = initializeApp(firebaseConfig);
export const db = getFirestore(app);
