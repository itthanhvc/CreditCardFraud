package sparkStreaming;

import java.util.List;
import java.util.Optional;

import sparkSQL.Account;
import sparkSQL.Transaction;

public class TransactionFraudDetection {

	public int HomeAddressRule(Account acct, Transaction currentTransaction) {
		String strHomeAddress = acct.getHomeAddress();
		String strLoc = currentTransaction.getLocation().toUpperCase();

		String[] address = strHomeAddress.split("\\|");
		int point = 0;
		if (!strLoc.contains(address[3].toUpperCase()))
			// Same country
			point += 11;
		else
		// Same city
		if (!strLoc.contains(address[1].toUpperCase()))
			point += 5;;

		return point;
	}

	public int RecentTransactionsRule(Transaction curTran, List<Transaction> tranList) {
		int factor2 = -1;
		boolean bolInLocationList = false;
		for (Transaction t : tranList) {
			if (t.getLocation().equals(curTran.getLocation())) {
				factor2 = 0;
				bolInLocationList = true;
				break;
			}
		}

		if (!bolInLocationList) {
			for (Transaction t1 : tranList) {
				String[] strCurLocation = curTran.getLocation().split("\\|");
				String[] strLoc = t1.getLocation().split("\\|");

				// Same country
				if (strCurLocation[3].toUpperCase().equalsIgnoreCase(
						strLoc[3].toUpperCase())) {
					factor2 = 5;
				} else
					factor2 = 11;
			}
		}

		return factor2;
	}

	public int AverageAmountRule(Transaction curTran, List<Transaction> tranList) {
		int factor3 = -1;
		Optional<Float> total = tranList.stream().map(s -> s.getAmount())
				.reduce((a, b) -> a + b);
		if(!total.isPresent())
			return 0;
		
		float avg = total.get() / tranList.size();
		if (curTran.getAmount() >= (avg + 500))
			factor3 = 5;
		else
			factor3 = 0;

		return factor3;
	}

	public String calcTotalPossibility(Account acct, Transaction curTran,
			List<Transaction> tranList) {

		int r1 = HomeAddressRule(acct, curTran);
		int r2 = RecentTransactionsRule(curTran, tranList);
		int r3 = AverageAmountRule(curTran, tranList);

		if (r1 + r2 + r3 <= 10)
			return "Enriched";

		return "Fraud";
	}
}
