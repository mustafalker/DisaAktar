const sql = require('mssql');
const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');

// SQL Server bağlantı ayarları
const config = {

};

// Dosya kaydetme yolu
const baseDir = path.join('C:', 'Users', 'ulker', 'Desktop', 'Mustafa ÜLKER', 'Sözleşmeler', 'Tamamı', 'Tekrar');
const baseFileName = 'SÖZLEŞMELER'; // Bu dosya adını değiştirin

// Bağlantıyı açıp kapatan fonksiyon
async function connectToSQL() {
    let pool;

    try {
        // Bağlantıyı aç
        pool = await sql.connect(config);
        console.log('SQL Server bağlantısı başarıyla açıldı!');

        const result = await pool.request().query("select * from vwSozlesmeDısaAktar ORDER BY DONEM DESC, FABRIKA_KODU ASC;");
        console.log(result.recordset.length + ' kayıt bulundu.');

        // Satırları gruplara ayır
        const chunkSize = 10000;
        let chunkIndex = 1;

        for (let i = 0; i < result.recordset.length; i += chunkSize) {
            const chunk = result.recordset.slice(i, i + chunkSize);

            // Excel çalışma kitabı (workbook) ve çalışma sayfası
            const wb = xlsx.utils.book_new();
            const ws = xlsx.utils.json_to_sheet(chunk);
            xlsx.utils.book_append_sheet(wb, ws, `SÖZLEŞMELER`);

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
