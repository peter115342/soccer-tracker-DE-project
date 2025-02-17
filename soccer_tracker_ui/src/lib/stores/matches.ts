import { writable } from 'svelte/store';
import { collection, query, where, getDocs } from 'firebase/firestore';
import { db } from '$lib/firebase';

interface Weather {
    apparent_temperature: number;
    temperature: number;
    precipitation: number;
    wind_speed: number;
    cloud_cover: number;
    humidity: number;
    weathercode: number;
}

interface League {
    id: number;
    name: string;
    logo: string;
}

interface Match {
    id: string;
    match_id: number;
    date: string;
    status: string;
    home_team: string;
    home_team_id: number;
    away_team: string;
    away_team_id: number;
    home_team_logo: string;
    away_team_logo: string;
    home_score: number;
    away_score: number;
    league: League;
    weather: Weather;
    venue: string;
    location: {
        lat: number;
        lon: number;
    };
    last_updated: string;
}

export const matches = writable<Match[]>([]);

export const fetchMatchesByDate = async (date: Date) => {
    const startDate = new Date(date);
    startDate.setHours(0, 0, 0, 0);
    
    const endDate = new Date(date);
    endDate.setHours(23, 59, 59, 999);

    const matchesCollection = collection(db, 'matches');
    const q = query(
        matchesCollection,
        where('date', '>=', startDate.toISOString()),
        where('date', '<=', endDate.toISOString())
    );

    try {
        const snapshot = await getDocs(q);
        const matchList = snapshot.docs.map(doc => ({
            id: doc.id,
            ...doc.data()
        })) as Match[];
        
        matches.set(matchList);
        console.log('Fetched matches for date:', date, matchList);
    } catch (error) {
        console.error('Error fetching matches:', error);
        matches.set([]);
    }
};
