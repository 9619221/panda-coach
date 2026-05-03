// Import 11-行情分析篇 docx → txt
const fs = require('fs');
const path = require('path');
const mammoth = require('mammoth');

const SRC = 'C:\\Users\\Administrator\\Desktop\\商品文件夹\\【批量下载】01.20220506-【复训】-分析行情挖掘机会.doc等\\熊猫学社 熊猫交易学社 黄金VIP 系统课合集\\006 熊猫学社\\「熊猫学社」熊猫交易学社 黄金VIP 系统课11-行情分析篇 54集';
const OUT = 'C:\\Users\\Administrator\\.claude\\skills\\panda-coach\\references\\knowledge\\transcripts\\11-行情分析篇';

if (!fs.existsSync(OUT)) fs.mkdirSync(OUT, { recursive: true });

const files = fs.readdirSync(SRC).filter(f => f.endsWith('.doc') || f.endsWith('.docx'));
files.sort();

(async () => {
  let ok = 0, fail = 0;
  for (const fname of files) {
    // Pattern: "NN.YYYYMMDD-【tag】[-]title.doc"  or  "NN. YYYYMMDD-..."
    const m = fname.match(/^\s*(\d{1,3})\.\s*(\d{8})-?【([^】]+)】-?(.+?)\.docx?$/);
    if (!m) { console.log('skip:', fname); fail++; continue; }
    const [, seq, date, tag, title] = m;
    const epNum = String(parseInt(seq, 10)).padStart(2, '0');
    const isRerun = tag.includes('复训');
    const cleanTitle = title.trim().replace(/[\\\/:*?"<>|]/g, '_');
    const outName = `11-${epNum}-${cleanTitle}${isRerun ? '-复训' : ''}.txt`;
    const outPath = path.join(OUT, outName);
    try {
      const result = await mammoth.extractRawText({ path: path.join(SRC, fname) });
      const text = result.value.trim();
      if (text.length < 50) { console.log('empty:', fname); fail++; continue; }
      fs.writeFileSync(outPath, `# 11-${epNum} ${cleanTitle}${isRerun ? ' [复训]' : ''}\n# 日期: ${date}\n\n${text}\n`, 'utf8');
      console.log(`OK ${epNum}: ${outName} (${text.length} chars)`);
      ok++;
    } catch (e) {
      console.log('err:', fname, e.message);
      fail++;
    }
  }
  console.log(`\nDone. ${ok} success, ${fail} fail.`);
})();
