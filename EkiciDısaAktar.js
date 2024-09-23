const sql = require('mssql');
const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');

// SQL Server bağlantı ayarları
const config = {

};

// Dosya kaydetme yolu
const baseDir = path.join('C:', 'Users', 'ulker', 'Desktop', 'Mustafa ÜLKER', 'Ekiciler', 'CanlıEkicilerTamamı');
const baseFileName = 'EKICILER'; // Bu dosya adını değiştirin

// Bağlantıyı açıp kapatan fonksiyon
async function connectToSQL() {
    let pool;

    const polygonRegex = /POLYGON\s*\(\(.*?\)\)/gm;
    const multiRegex = /MULTIPOLYGON\s*\(\(\(.*?\)\)\)/gm;

    try {
        // Bağlantıyı aç
        pool = await sql.connect(config);
        console.log('SQL Server bağlantısı başarıyla açıldı!');

        const result = await pool.request().query("select * from vwEkıcıDısaAktar");
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
        const chunkSize = 30000;
        let chunkIndex = 1;

        for (let i = 0; i < result.recordset.length; i += chunkSize) {
            const chunk = result.recordset.slice(i, i + chunkSize);

            // Excel çalışma kitabı (workbook) ve çalışma sayfası
            const wb = xlsx.utils.book_new();
            const ws = xlsx.utils.json_to_sheet(chunk);
            xlsx.utils.book_append_sheet(wb, ws, `EKİCİLER`);

            // Dosya adını oluştur ve kaydet
            const fileName = `${baseFileName}_part${chunkIndex}.xlsx`;
            const filePath = path.join(baseDir, fileName);

            // Excel dosyasını kaydet
            xlsx.writeFile(wb, filePath);
            console.log(`Dosya ${filePath} olarak kaydedildi.`);

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
