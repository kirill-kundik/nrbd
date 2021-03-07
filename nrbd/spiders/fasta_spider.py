import re

import scrapy
import scrapy_splash


class FastaSpider(scrapy.Spider):
    BASE_URL = 'https://www.ncbi.nlm.nih.gov'

    name = 'fasta'
    start_urls = [f'{BASE_URL}/popset/429846592', ]
    result_file = 'result.csv'

    def start_requests(self):
        with open(self.result_file, 'w') as f:
            f.write('version,region,fasta\n')
        for url in self.start_urls:
            yield scrapy_splash.SplashRequest(url, self.parse, args={'wait': 10})

    def parse_nuccore(self, response, **kwargs):
        try:
            fasta_full = response.css('div#viewercontent1 > pre::text').extract()[0]
        except IndexError:
            return scrapy_splash.SplashRequest(response.url, self.parse_nuccore, args={'wait': 3.0})

        version = fasta_full[1:11]
        region_rex = r'isolate ([A-Z]+)'
        fasta_rex = r'mitochondrial([A-Z]{377})'
        region = re.compile(region_rex).search(fasta_full).group(1)
        fasta = re.compile(fasta_rex).search(fasta_full.replace('\n', '')).group(1)

        with open(self.result_file, 'a') as f:
            f.write(f'{version},{region},{fasta}\n')

        yield {
            'region': region,
            'version': version,
            'fasta': fasta
        }

    def parse(self, response, **kwargs):
        for url in response.css('ul.psaccn > li > a::attr(href)').extract():
            yield scrapy_splash.SplashRequest(
                f'{self.BASE_URL}{url}?report=fasta', self.parse_nuccore, args={'wait': 1.5}
            )
