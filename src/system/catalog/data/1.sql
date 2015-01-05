--upgrade from 0 to 1

alter table "instance"
    add column path varchar;

alter table "cluster"
    add column metadata_version integer;

create function get_metadata_version() returns integer as $$
declare v integer;
begin
  select into v metadata_version from "cluster" limit 1;
  return v;
end;
$$ language plpgsql;

update "cluster" set metadata_version = 1;
