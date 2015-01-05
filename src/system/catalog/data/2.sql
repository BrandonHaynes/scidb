--upgrade from 1 to 2

alter table "array_dimension"
    alter column chunk_interval set data type bigint,
    alter column chunk_overlap set data type bigint;

update "cluster" set metadata_version = 2;
