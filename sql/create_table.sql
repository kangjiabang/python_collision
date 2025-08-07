DROP TABLE IF EXISTS hzdk_buildings;

CREATE TABLE IF NOT EXISTS hzdk_buildings
(
    gid integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    building_id bigint ,
    building_type character varying(80) ,
    building_name character varying(80) ,
    building_addr character varying(512),
    area_code character varying(20) ,
    geom geometry(MultiPolygon, 4326) ,
    building_height numeric(10,2) ,
    create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 表注释
COMMENT ON TABLE hzdk_buildings IS '余杭区建筑物信息表';

-- 字段注释（更标准方式，兼容所有客户端工具）
COMMENT ON COLUMN hzdk_buildings.building_id IS '建筑物id';
COMMENT ON COLUMN hzdk_buildings.building_type IS '建筑物类型';
COMMENT ON COLUMN hzdk_buildings.building_name IS '建筑物名称';
COMMENT ON COLUMN hzdk_buildings.building_addr IS '建筑物地址';
COMMENT ON COLUMN hzdk_buildings.area_code IS '区域编码';
COMMENT ON COLUMN hzdk_buildings.geom IS '建筑物坐标';
COMMENT ON COLUMN hzdk_buildings.building_height IS '建筑物高度';
COMMENT ON COLUMN hzdk_buildings.create_time IS '创建时间';
COMMENT ON COLUMN hzdk_buildings.update_time IS '更新时间';

-- 创建 GIST 索引
CREATE INDEX IF NOT EXISTS hzdk_buildings_geom_idx
    ON hzdk_buildings USING gist (geom);
CREATE INDEX idx_hzdk_buildings_geom_geog ON hzdk_buildings USING GIST (geography(geom));

-- 创建触发器函数：自动更新 update_time
CREATE OR REPLACE FUNCTION update_hzdk_buildings_modtime()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器：在 UPDATE 时自动更新 update_time 字段
DROP TRIGGER IF EXISTS trg_update_time ON hzdk_buildings;

CREATE TRIGGER trg_update_time
BEFORE UPDATE ON hzdk_buildings
FOR EACH ROW
EXECUTE FUNCTION update_hzdk_buildings_modtime();


