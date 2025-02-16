import { writable } from 'svelte/store';
import { collection, getDocs } from 'firebase/firestore';
import { db } from '$lib/firebase';

interface Match {
    match_id: number;
    kickoff: string;
    status: string;
    home_team: string;
    away_team: string;
    competition_id: number;
    competition_name: string;
}

interface DayMatches {
    date: string;
    matches: Match[];
    last_updated: string;
}

export const upcomingMatches = writable<DayMatches[]>([]);

export const fetchUpcomingMatches = async () => {
    const matchesCollection = collection(db, 'upcoming_matches');
    const snapshot = await getDocs(matchesCollection);
    const matches = snapshot.docs.map(doc => ({
        date: doc.id,
        ...doc.data()
    })) as DayMatches[];
    upcomingMatches.set(matches);
};
