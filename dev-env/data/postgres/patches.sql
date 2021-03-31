-- This file contains patches to the database, to take care of data that was added by mistake

-- Remove the Portuguese Empire, which is marked as a "super country", there are other countries
-- that have P17 edges to the Portuguese Empire, which confuses the queries - we do not assume
-- countries can belong to other countries

DELETE FROM edges WHERE node2='Q200464' or node1='Q200464'
