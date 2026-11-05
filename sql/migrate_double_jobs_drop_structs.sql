-- Only needed if structA/structB were already added to the database.
ALTER TABLE tmp_doubles_letchecked
    DROP COLUMN IF EXISTS structA,
    DROP COLUMN IF EXISTS structB;
