-- This file contains patches to the database, to take care of data that was added by mistake

-- Remove the Portuguese Empire, which is marked as a "super country", there are other countries
-- that have P17 edges to the Portuguese Empire, which confuses the queries - we do not assume
-- countries can belong to other countries

DELETE FROM edges WHERE node2='Q200464' or node1='Q200464';


-- Remove qnodes with multiple labels

delete from edges where id='"Q14818820-label-""Fragile States Index""-0000"';
delete from edges where id='Q8035788-label-"economic fitness dataset"-0000';
delete from edges where id='Q8035788-label-"World Development Indicators"-0000';

-- Add qndoes

insert into edges values   ('Q7184-label-0', 'Q7184', 'label', 'NATO''@en', 'language_qualified_string');
insert into strings values ('Q7184-label-0', 'NATO', 'en');
