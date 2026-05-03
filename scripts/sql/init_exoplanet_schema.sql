if object('dbo.stg_exoplanets_raw', 'U') is null
begin
    create table dbo.stg_exoplanets_raw (
        id bigint IDENTITY(1,1) PRIMARY KEY,
        pl_name nvarchar(255) not NULL,
        hostname nvarchar(255) NULL,
        discoverymethod nvarchar(255) NULL,
        disc_year int NULL,
        disc_pubdate date NULL,
        pl_orbper float NULL,
        pl_rade float NULL,
        pl_bmasse float NULL,
        st_teff float NULL,
        sy_dist float NULL,
        source_row_hash char(64) not NULL,
        source_run_id bigint not NULL,
        load_dttm datetime2(3) not NULL default SYSUTCDATETIME()
    );
end;
GO

if object_id('dbo.dim_exoplanets', 'U') is NULL
begin
    create table dbo.dim_exoplanets (
        id bigint IDENTITY(1,1) PRIMARY KEY,
        pl_name nvarchar(255) not NULL,
        hostname nvarchar(255) NULL,
        discoverymethod nvarchar(255) NULL,
        disc_year int NULL,
        disc_pubdate date NULL,
        pl_orbper float NULL,
        pl_rade float NULL,
        pl_bmasse float NULL,
        st_teff float NULL,
        sy_dist float NULL,
        source_row_hash cahr(64) not NULL,
        first_seen_dttm datetime2(3) not NULL default SYSUTCDATETIME(),
        last_seen_dttm datetime2(3) not NULL default SYSUTCDATETIME(),
        is_current bit not NULL default 1
    );
end;
GO

if not exists (select 1 from sys.indexes
                where name = 'UX_dim_exoplanets_pl_name'
                    and object_id = object_id('dbo.dim_exoplanets'))
begin
    create unique index UX_dim_exoplanets_pl_name
        on dbo.dim_exoplanets(pl_name);
end;
GO

if not exists (select 1 from sys.indexes
                where name = 'IX_dim_exoplanets_disc_pubdate'
                    and object_id = object_id('dbo.dim_exoplanets'))
begin
    create index IX_dim_exoplanets_disc_pubdate
        on dbo.dim_exoplanets(disc_pubdate);
end;
GO

if object_id('dbo.etl_log', 'U') is NULL
begin
    create table dbo.etl_log (
        id bigint IDENTITY(1,1) PRIMARY KEY,
        entity_name nvarchar(100) not NULL,
        start_time datetime2(3) not NULL,
        end_time datetime2(3) NULL,
        status nvarchar(20) not NULL,
        rows_loaded int NULL,
        watermark_before datetime2(3) NULL,
        watermark_after datetime2(3) NULL,
        error_message nvarchar(4000) NULL
    );
end;
GO

if object_id('dbo.vw_exoplanets_latest', 'V') is not NULL
begin
    drop view dbo.vw_exoplanets_latest;
end;
GO

create view dbo.vw_exoplanets_latest as
select
    pl_name as planet_name,
    hostname as host_star_name,
    discoverymethod as discovery_method,
    disc_year as discovery_year,
    disc_pubdate as discovery_publication_date,
    pl_orbper as orbital_period_days,
    pl_rade as planet_radius_earth,
    pl_bmasse as planet_mass_earth,
    st_teff as star_temperature_kelvin,
    sy_dist as system_distance_parsec,
    first_seen_dttm,
    last_seen_dttm
from dbo.dim_exoplanets
where is_current = 1;
GO
