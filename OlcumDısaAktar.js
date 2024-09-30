const sql = require('mssql');
const fs = require('fs');
const path = require('path');
const json2csv = require('json2csv');

// SQL Server bağlantı ayarları
const config = {

};

// Dosya kaydetme yolu
const baseDir = path.join('C:', 'Users', 'ulker', 'Desktop', 'Mustafa ÜLKER', 'Ölçümler', '2021', '2021_EKSİK_KALANLAR');
const baseFileName = '2021_EKSİK_KALANLAR'; // Bu dosya adını değiştirin

// Bağlantıyı açıp kapatan fonksiyon
async function connectToSQL() {
    let pool;

    const polygonRegex = /POLYGON\s*\(\(.*?\)\)/gm;
    const multiRegex = /MULTIPOLYGON\s*\(\(\(.*?\)\)\)/gm;

    try {
        // Bağlantıyı aç
        pool = await sql.connect(config);
        console.log('SQL Server bağlantısı başarıyla açıldı!');

        const result = await pool.request().query("select * from [2021_EKSİKLER]");
        console.log(result.recordset.length + ' kayıt bulundu.');

        // GEOMETRY_TEXT verisini işleme
        result.recordset.map((item) => {
            if (item.GEOMETRY_TEXT && item.GEOMETRY_TEXT.startsWith('GEOMETRYCOLLECTION')) {
                const wkt_arr = item.GEOMETRY_TEXT.match(polygonRegex) || [];
                const longestItem = wkt_arr.reduce((longest, current) => {
                    return current.length > longest.length ? current : longest;
                }, "");
                item.GEOMETRY_TEXT = longestItem;
            }
        });

        // Satırları gruplara ayır
        const chunkSize = 2900;
        let chunkIndex = 1;

        for (let i = 0; i < result.recordset.length; i += chunkSize) {
            const chunk = result.recordset.slice(i, i + chunkSize);

            // CSV'ye dönüştürme
            const csv = json2csv.parse(chunk);
            const fileName = `${baseFileName}_part${chunkIndex}.csv`;
            const filePath = path.join(baseDir, fileName);

            // CSV dosyasını yazma
            fs.writeFile(filePath, csv, function(err) {
                if (err) {
                    console.log(`Dosya kaydedilirken hata oluştu: ${err}`);
                } else {
                    console.log(`Dosya ${filePath} olarak kaydedildi.`);
                }
            });

            // Boş klasör oluşturma
            const emptyFolderName = `${baseFileName}_part${chunkIndex}`;
            const emptyFolderPath = path.join(baseDir, emptyFolderName);
            if (!fs.existsSync(emptyFolderPath)) {
                fs.mkdirSync(emptyFolderPath);
                console.log(`Boş klasör oluşturuldu: ${emptyFolderPath}`);
            }

            chunkIndex++;
        }

    } catch (err) {
        console.error('Bağlantı hatası: ', err);
    } finally {
        // Bağlantıyı kapat
        if (pool) {
            await pool.close();
            console.log('SQL Server bağlantısı kapatıldı.');
        }
    }
}

// Fonksiyonu çalıştır
connectToSQL();