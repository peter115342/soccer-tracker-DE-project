import { writable } from 'svelte/store';
import { doc, getDoc } from 'firebase/firestore';
import { db } from '$lib/firebase';

export const matchSummary = writable<string | null>(null);

export async function fetchMatchSummary(date: string, league: string) {
	try {
		const normalizedLeague = league.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
		const dateObj = new Date(date);
		const dateOnly = dateObj.toISOString().split('T')[0];
		const docId = `${dateOnly}_${normalizedLeague}`;

		const summaryDoc = await getDoc(doc(db, 'match_summaries', docId));

		if (summaryDoc.exists()) {
			const data = summaryDoc.data();

			let processedContent = data.content.replace(/\\n/g, '\n');

			processedContent = processedContent.replace(/(?<!\n)#(?!\s*#)/g, '\n\n#');

			processedContent = processedContent.replace(/\n{3,}/g, '\n\n');

			processedContent = processedContent.replace(/\*\*Final Score.*?\*\*\n(?!\n)/g, '**$&\n\n');

			matchSummary.set(processedContent.trim());
		} else {
			console.log(`No summary found for ${docId}`);
			matchSummary.set(null);
		}
	} catch (error) {
		console.error('Error fetching match summary:', error);
		matchSummary.set(null);
	}
}
