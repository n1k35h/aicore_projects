/* Counting the length of each card_number and changed the column to the required data type */

-- SELECT * FROM dim_card_details;

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(20),
	ALTER COLUMN expiry_date TYPE VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE DATE;

SELECT index,
	card_number,
	LENGTH(card_number) as length,
	card_provider,
	expiry_date,
	date_payment_confirmed
FROM dim_card_details;