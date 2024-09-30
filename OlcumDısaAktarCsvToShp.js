const sql = require('mssql');
const fs = require('fs');
const path = require('path');
const json2csv = require('json2csv').parse;
const fse = require('fs-extra');
const shpwrite = require('shp-write');
const wellknown = require('wellknown');

// SQL Server bağlantı ayarları
const config = {

};

// Dosya kaydetme yolu
const baseDir = path.join('C:', 'Users', 'ulker', 'Desktop', 'Mustafa ÜLKER', 'Ölçümler', '2021');
const baseFileName = '2021_TAMAMI';

// Bağlantıyı açıp kapatan fonksiyon
async function connectToSQL() {
    let pool;

    try {
        // Bağlantıyı aç
        pool = await sql.connect(config);
        console.log('SQL Server bağlantısı başarıyla açıldı!');

        const result = await pool.request().query("SELECT * FROM [2021_TAMAMI]");
        console.log(result.recordset.length + ' kayıt bulundu.');

        // GEOMETRY_TEXT verisini işleme
        result.recordset.forEach((item) => {
            if (item.GEOMETRY_TEXT && item.GEOMETRY_TEXT.startsWith('GEOMETRYCOLLECTION')) {
                const polygonRegex = /POLYGON\s*\(\(.*?\)\)/gm;
                const multiPolygonRegex = /MULTIPOLYGON\s*\(\(\(.*?\)\)\)/gm;

                const wkt_arr = item.GEOMETRY_TEXT.match(polygonRegex) || item.GEOMETRY_TEXT.match(multiPolygonRegex) || [];
                item.GEOMETRY_TEXT = wkt_arr.length > 0 ? wkt_arr[0] : '';
            }
        });

        // Satırları gruplara ayır
        const chunkSize = 2900;
        let chunkIndex = 1;

        for (let i = 0; i < result.recordset.length; i += chunkSize) {
            const chunk = result.recordset.slice(i, i + chunkSize);

            // CSV'ye dönüştürme
            const csv = json2csv(chunk);
            const fileName = `${baseFileName}_part${chunkIndex}.csv`;
            const filePath = path.join(baseDir, fileName);

            // CSV dosyasını yazma
            fs.writeFileSync(filePath, csv);
            console.log(`Dosya ${filePath} olarak kaydedildi.`);

            // GeoJSON'u oluşturma
            const geojson = {
                type: "FeatureCollection",
                features: chunk.map(item => {
                    // GEOMETRY_TEXT verisini işleme ve doğru türü ayarlama
                    let geometryType = "Polygon";
                    let coordinates = [];

                    if (item.GEOMETRY_TEXT.startsWith('MULTIPOLYGON')) {
                        geometryType = "MultiPolygon";
                    }

                    try {
                        coordinates = parseWKT(item.GEOMETRY_TEXT);
                    } catch (e) {
                        console.error("Geometri işlenirken hata oluştu:", e);
                    }

                    const properties = { ...item };

                    return {
                        type: "Feature",
                        geometry: {
                            type: geometryType,
                            coordinates: coordinates
                        },
                        properties: properties
                    };
                }).filter(feature => feature.geometry.coordinates.length > 0) // Filter out invalid geometries
            };

            // Shapefile oluşturma
            const shapefileDir = path.join(baseDir, `${baseFileName}_part${chunkIndex}_shapefile`);
            fse.ensureDirSync(shapefileDir);

            await createShapefile(geojson, shapefileDir, `${baseFileName}_part${chunkIndex}`);
            chunkIndex++;
        }

    } catch (err) {
        console.error('Bağlantı hatası: ', err);
    } finally {
        if (pool) {
            await pool.close();
            console.log('SQL Server bağlantısı kapatıldı.');
        }
    }
}

// Shapefile ve gerekli dosyaları oluşturma fonksiyonu
async function createShapefile(geojson, shapefileDir, shapefileName) {
    try {
        // Shapefile verisini yazma
        shpwrite.write(
            geojson,
            { folder: shapefileDir, types: { polygon: shapefileName } },
            (err, files) => {
                if (err) {
                    console.error("Shapefile oluşturulurken hata:", err);
                    return;
                }

                // Dosyaları kaydetme
                for (const name in files) {
                    const filePath = path.join(shapefileDir, `${shapefileName}.${name}`);
                    fs.writeFileSync(filePath, files[name]);
                    console.log(`Dosya başarıyla kaydedildi: ${filePath}`);
                }
            }
        );

    } catch (err) {
        console.error("Shapefile oluşturulurken hata:", err);
    }
}

// WKT formatını GeoJSON koordinatlarına dönüştüren yardımcı fonksiyon
function parseWKT(wkt) {
    const geometry = wellknown(wkt);

    if (!geometry || !geometry.coordinates || geometry.coordinates.length === 0) {
        throw new Error('Invalid WKT or empty geometry');
    }

    return geometry.coordinates;
}

// Fonksiyonu çalıştır
connectToSQL();