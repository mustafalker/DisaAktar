const sql = require('mssql');
const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');

// SQL Server bağlantı ayarları
const config = {

};

// Dosya kaydetme yolu
const baseDir = path.join('C:', 'Users', 'ulker', 'Desktop', 'Mustafa ÜLKER', 'Ölçümler', '2021', 'DÜZELTİLENLER');
const baseFileName = '2021_TAMAMI'; // Bu dosya adını değiştirin

// Bağlantıyı açıp kapatan fonksiyon
async function connectToSQL() {
    let pool;

    const polygonRegex = /POLYGON\s*\(\(.*?\)\)/gm;
    const multiRegex = /MULTIPOLYGON\s*\(\(\(.*?\)\)\)/gm;

    try {
        // Bağlantıyı aç
        pool = await sql.connect(config);
        console.log('SQL Server bağlantısı başarıyla açıldı!');

        const result = await pool.request().query(`
            SELECT 1 AS COMPANYREF,
                   e.TC_KIMLIK_NO AS TC_KIMLIK_,
                   c.TC_KIMLIK_NO AS CAVUS_TC_K,
                   ISNULL(eo.MUCADELE_DEKAR, 0) AS MUCADELEDEKAR,
                   eo.TAHRIBAT_TURU_REF AS TAHRIBATTURU,
                   eo.FABRIKA_KODU,
                   eo.GLOBALID,
                   c.GLOBALID AS CAVUS_GLOBAL_ID,
                   s.FABRIKA_ADI AS FABRIKAADI,
                   b.BOLGEADI AS BOLGE_ADI,
                   k.ILADI AS IL_ADI,
                   k.ILCEADI AS ILCE_ADI,
                   k.KOYADI AS KOY_ADI,
                   eo.KANTARADI AS KANTAR_ADI,
                   eo.KANTARREF,
                   eo.BOLGEADI,
                   eo.BOLGEREF,
                   eo.CAVUSREF,
                   eo.SOZLESMEREF,
                   eo.URUN_GELISIMI_REF,
                   eo.OLCUMREF,
                   eo.KOYREF,
                   eo.KOYADI,
                   es.SIRA_NO AS SIRANO,
                   e.ADI + ' ' + e.SOYADI AS EKICIADI,
                   eo.FEATUREID,
                   eo.ADA,
                   eo.PARSEL,
                   eo.OLCUM_TARIHI AS OLCUM_TARI,
                   eo.EKIM_TARIHI AS EKIM_TARIH,
                   c.AD AS CAVUS_ADI,
                   c.SOYAD AS CAVUS_SOYA,
                   f.FEATUREID AS FABRIKAREF,
                   s.GRUP_NO AS SOZLESME_G,
                   eo.ILK_EKIM_DEKAR AS TARLA_DEKA,
                   eo.UPDATED_USERID,
                   eo.TAAHHUT_OLCUM,
                   eo.TARLA_SIRA_NO,
                   eo.PARSEL_DEKAR,
                   eo.INSERTED_TIME,
                   eo.DONEM,
                   eo.TOHUM_CESIDI_REF AS TOHUM_TIPI,
                   eo.URUN_GELISIMI_REF AS URUNGELISIMI,
                   ISNULL(eo.ILK_EKIM_DEKAR, 0) AS ILKEKIMDEKAR,
                   ISNULL(eo.SIRKET_MIBZERI_DEKAR, 0) AS SIRKETMIBZERIDEKAR,
                   ISNULL(eo.KENDI_MIBZERI_DEKAR, 0) AS KENDIMIBZERIDEKAR,
                   ISNULL(eo.PNOMATIK_DEKAR, 0) AS PNOMATIKDEKAR,
                   ISNULL(eo.TAHRIP_DEKAR, 0) AS TAHRIPDEKAR,
                   ISNULL(eo.TAHRIP_DEKAR_2, 0) AS TAHRIPDEKAR2,
                   ISNULL(eo.MUKERRER_DEKAR, 0) AS MUKERRERDEKAR,
                   ISNULL(eo.URUN_TASIYAN_DEKAR, 0) AS URUNTASIYANDEKAR,
                   eo.TOHUM_CESIDI_REF AS TOHUMCESIDI,
                   eo.MIBZER_TIPI_REF AS MIBZERTIPI,
                   eo.EKIM_ARALIK_REF AS EKIMARALIGI,
                   eo.MUCADELE_YAPAN AS MUCADELEYAPAN,
                   eo.MUCADELE_TURU AS MUCADELETURU,
                   eo.TRAKTORCU,
                   ISNULL(eo.ACIKLAMA, '') AS ACIKLAMA
            FROM EKICI_OLCUM eo
                     LEFT JOIN SOZLESME s ON s.FABRIKA_KODU = eo.FABRIKA_KODU AND s.FEATUREID = eo.SOZLESMEREF
                     LEFT JOIN EKICI_SOZLESME es ON es.FABRIKA_KODU = eo.FABRIKA_KODU AND es.SOZLESMEID = eo.SOZLESMEREF AND es.EKICIID = eo.EKICIREF
                     LEFT JOIN EKICI e ON e.FABRIKA_KODU = eo.FABRIKA_KODU AND e.FEATUREID = eo.EKICIREF
                     LEFT JOIN KOY_ALL k ON k.FEATUREID = eo.KOYREF
                     LEFT JOIN CAVUS c ON c.FEATUREID = eo.CAVUSREF AND c.FABRIKA_KODU = eo.FABRIKA_KODU
                     LEFT JOIN BOLGE b ON b.BOLGEKODU = eo.BOLGEREF AND b.FABRIKAKODU = eo.FABRIKA_KODU
                     LEFT JOIN KANTAR ka ON ka.KANTARKODU = eo.KANTARREF AND ka.FABRIKAKODU = eo.FABRIKA_KODU
                     LEFT JOIN FABRIKA f ON f.FABRIKAKODU = eo.FABRIKAREF
                WHERE eo.DONEM = 2021 
            GROUP BY eo.FABRIKA_KODU,
                     eo.GLOBALID,
                     c.GLOBALID,
                     s.FABRIKA_ADI,
                     b.BOLGEADI,
                     k.ILADI,
                     k.ILCEADI,
                     k.KOYADI,
                     ka.KANTARADI,
                     eo.KANTARADI,
                     eo.KANTARREF,
                     eo.BOLGEADI,
                     eo.BOLGEREF,
                     eo.CAVUSREF,
                     eo.SOZLESMEREF,
                     eo.URUN_GELISIMI_REF,
                     eo.OLCUMREF,
                     eo.KOYREF,
                     eo.KOYADI,
                     e.TC_KIMLIK_NO,
                     c.TC_KIMLIK_NO,
                     es.SIRA_NO,
                     e.ADI + ' ' + e.SOYADI,
                     eo.FEATUREID,
                     eo.ADA,
                     eo.PARSEL,
                     eo.OLCUM_TARIHI,
                     eo.EKIM_TARIHI,
                     c.AD,
                     c.SOYAD,
                     f.FEATUREID,
                     s.GRUP_NO,
                     eo.ILK_EKIM_DEKAR,
                     eo.UPDATED_USERID,
                     eo.TAAHHUT_OLCUM,
                     eo.TARLA_SIRA_NO,
                     eo.PARSEL_DEKAR,
                     eo.INSERTED_TIME,
                     eo.DONEM,
                     eo.TOHUM_CESIDI_REF,
                     ISNULL(eo.ILK_EKIM_DEKAR, 0),
                     ISNULL(eo.SIRKET_MIBZERI_DEKAR, 0),
                     ISNULL(eo.KENDI_MIBZERI_DEKAR, 0),
                     ISNULL(eo.PNOMATIK_DEKAR, 0),
                     eo.TAHRIBAT_TURU_REF,
                     ISNULL(eo.TAHRIP_DEKAR, 0),
                     ISNULL(eo.TAHRIP_DEKAR_2, 0),
                     ISNULL(eo.MUKERRER_DEKAR, 0),
                     ISNULL(eo.URUN_TASIYAN_DEKAR, 0),
                     eo.MIBZER_TIPI_REF,
                     eo.EKIM_ARALIK_REF,
                     eo.MUCADELE_YAPAN,
                     eo.MUCADELE_TURU,
                     ISNULL(eo.MUCADELE_DEKAR, 0),
                     eo.TRAKTORCU,
                     ISNULL(eo.ACIKLAMA, '')
        `);

        console.log(result.recordset.length + ' kayıt bulundu.');

        // GEOMETRY_TEXT verisini işleme
        result.recordset.map((item) => {
            if (item.GEOMETRY_TEXT && item.GEOMETRY_TEXT.startsWith('POLYGON')) {
                const matches = item.GEOMETRY_TEXT.match(polygonRegex);
                item.GEOMETRY_TEXT = matches ? matches[0] : null;
            } else if (item.GEOMETRY_TEXT && item.GEOMETRY_TEXT.startsWith('MULTIPOLYGON')) {
                const matches = item.GEOMETRY_TEXT.match(multiRegex);
                item.GEOMETRY_TEXT = matches ? matches[0] : null;
            }
        });

        // Excel dosyasını oluşturma
        const workbook = xlsx.utils.book_new();
        const worksheet = xlsx.utils.json_to_sheet(result.recordset);

        // Sayfayı çalışma kitabına ekleyin
        xlsx.utils.book_append_sheet(workbook, worksheet, 'Data');

        // Excel dosyasını kaydetme
        const filePath = path.join(baseDir, `${baseFileName}.xlsx`);
        xlsx.writeFile(workbook, filePath);
        console.log('Excel dosyası başarıyla kaydedildi: ' + filePath);

    } catch (err) {
        console.error('SQL Server bağlantısı veya sorgu hatası:', err);
    } finally {
        if (pool) {
            await pool.close();
            console.log('SQL Server bağlantısı kapatıldı.');
        }
    }
}

// Bağlantıyı aç ve işlemi başlat
connectToSQL();
