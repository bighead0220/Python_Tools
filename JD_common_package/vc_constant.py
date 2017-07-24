#!/usr/bin/env python3
"""
USAGE: 常用变量
AUTHOR: liuxiaoze
MODIFIED BY: liuxiaoze
MODIFIED TIME: 2017年1月10日
"""

log_path = '/home/mart_vdp/task/edw/etl/app/bin/vc/logs'
db_name = "app"
temp_db_name = "dev"
dim_db_name = "dim"

#指定推数据的供应商编码 格式  '123','1234'
provider_codes = "'nbftyx','flpzgtz','ljdzzgyxgs','hrjtdzsw','lszxdzkj','qdhxdqyx','11026','ahkjtc','aosmith','bfgjjtjj','bjbcjy','bjbkbs','bjbs','bjbstpsm','bjbsyk','bjbxdsm','bjdxkhdz','bjgkhxkj','bjglty','bjjbqysm','bjjdhykm','bjjdtd','bjjmjdz','bjkbzy','bjlkshn','bjlrkj','bjlrl','bjmdqy','BJMHWY','bjptbxkj','bjrxwy','bjshlhtkj','bjshyd','bjssxx','bjsyh','bjtczxmy','bjwfkjy','bjwtfjsfz','bjwzdsm','bjxld','bjxyzmsm','bjzhrt','bjzkzy','bjzrt','bjztct','bjzywy','bjzyzg','bmdqzh','bojtdqtj','brdjsxty','brsd','bsddgj','bsgjgf','btdqsh','bxjydq','cqmhgl','cxdzsw','cxftdq','dgssd','fjcgafjs','flmdz','fsgld','fsjsyl','fssdsx','fssfydzsw','fssnhbz ','fssok','fssoqdq','fsssdqhs','fsszcdq','fsyx','galanz','gdamdq','gddeer','gddldq','gdhxbxqd','gdmdzl','gdwhxdq','gdxxdq','gdzk','gzgxdq','gzsjhwj','gzsqh','gzssxx','gzsyc','gzsyd','gzzgkt','hbnhgc','hersdsy','hfmdrsd','hfml','hfrsd','hjdedkj','hnssxxgl','hzky','hztclzm','hzzgdq','jjblbjzs','jldqyxgs','jrsp','jsdfyy','JSDGKJ　','kjjtsz','lsbjgdgc','mlgjsy','nbakskt','nblmysh','nbxmdq','nbzz','nbzz ','ndjtdzsw','njcwjydq ','nlzg','opzm','qdakmdq','qdakmds','qdhsdq','qdhxdqyx','qdhxktgf','qdjxd','qfzggjxs ','qyjtgf','qysy','RSDXJD','scchdq','scchkt','sdlchd','shamdz','shbjmyfz ','shftmy','shhymy','shlklkmy','shlt','shlysy','shmmmyyxgs','shpksy','shrlsy','shyftz','shzcsyyxgs','sxmhch','SXZGSH','syldsm','szchuangw','szher','szhsjx','szshymy','szszmdkj','tgmy','TYTZ','wxxte','xashlzdq','xcxfgl','xlfbx','xmqqgd','yhwczggf','yjlhhbshb','yjwysh','ymmj','zjkndq','zjqyscl','zjsk','zjxxjd','zmbxkj','zsams','zsdrj','zssl','zssldjydq','zssqtt','zzhc','nbhp','jsshdz','snzhg','xpsmzg','sxzgtzyxgs','fsssdqky','qdymks','bjlklssm','bjcg','bjqshwykj','rbmjsm','bjrssh','hmgjmy','ylksxjd','bjbshp','bjyxcy','bjybht','sydqjt','amtdq','gdtjdq','kwsdq','szjsbsm','ypmysh','lxydtxwh','fsskjl','zssqw','zsxtdq','czymdq','jrsp','sxhrd','ylsh','bjwpsm','bjdml','fzsyjmy','mmsyyzb','whmt','bjkls','shrhsyfz','szsyb','zhlxnby','bjxyjhsm','bjztjdl','cqdy','gzsyxl','gzhshgyymy','shaqh','shxtmy','cqdy','bjhsx','fcwy','shzhongyaomy','szgzb','shenzhenshbnshy','szslykjgs','rychsm','szsgl','flpzgtz','gzhsy','bjjzxsy','shyydz','fslspmy','ynjsh','bjwmxchjmfzh','shfkdq','bjdfhtjm','njhysm','zhshshlfyy','bjyh','bjsjzy','zsslt','shfyxx','szswkj','bjwld','gzfcmy','njbst','bjtpdz','bjszwlkj','bfmy','shpr','nblgsx','kshjdz','szsllkj','shzbyx','aqshsm','shdymls','szshnjm','bmdqzh','bjbxaa','bjmjjx','bjhpzh','bjlshtshm','shwlsm','gzybmy','bhrmy','bjmssh','szsaj','bjhmjsmfz','bjasdd','gzfnmy','gzxj','bjlcx','bjzhe','dltfsy','bjrsby','bjjzxsy','shdfrfxs','gzswrgz','hjhpsc','whcj','lnbfhzp','hebzyr','lbaks','wltj','ljsjh','lydmdz','shkcsm','bjytzh','bjdfhtjm','yljh','gzbkdz','jblzg','hbsb','zqhc','bjhswd','shqh','zsaj','bjbst','hnqjwsyp','YNJSH','jygzryp','bjspa','bjfcgm','shyqh','jxxssm','zyszqg','bjysmj','wte','bjyyxy','njal','bjyslfyyfgs','hcyyyx','sxbjmy','bjxwhsm','zjmlg','bjssjhmy','shenzljy','mljf','gzhanhou','shywen','zybjtyxysm','gzhbsw','bjmnmy','gzmk','ynfynf','csxmh','shrssm','hnsyj','bjyhrssm','jljt','bjjyzr','bjxbw','bjzxhtsm','bjcj','Mattel','bjyhjy','shzqsmy','szsjwh','bjfylm','whfc','bjmmzh','bjfhbysm','gdafdm','szwmdzsw','hwkj','shxhsm','stjx','bjsyjm','njanu','hzch','zgdxzjwk','bjqfkh','jnzg','whadm','ypsh','gzxsdn','bjjjcx','gdxydnkj','cdyqxzlkj','yxkjzg','szshj','bjej','hfmtyzdzkj','shxsdzyxgs','shzzhy','bjsqsj','leyard','bjbbk','hwzdyx','xiaomi','hwzddg','bjzqhdkj','baidu','qzrjbj','chdfyxkjkf','szsdte','shzhshdcshy','szsqxwj','atwx','bjzdzc','hsdn','bjrzhz','gsjht','gzajkj','dgshmdz','bjszsm','bjlzb','shzgdz','lydzhz','lxbj','bjshdwegj','bjjmdzjs','bjmctx','sqgjmysh','bjszjy','dgsxhxdz','bjrzsw','hlbe','bjjdsmkj','hldnkjsz','szsqyy','qjdzkjsh','tkdz','szaakj','hsykj','zjzn','fzddn','xmmxb','bdwl','liyuda','jdsh','shzhytxxkj','gztd','shjhsm','szlms','ymkj','bjjtht','bjjzfs','bjzt','zmdz','szjxtd','ljzgkj','mjdtyxgs','lxbj','ymdzsm','gzhsbg','fohc','szsqxwj','shszsm','lcmy','bjxmxdsm','AML','bjykmy','bjmctx','zgccjsj','dgsyy','wygj','hangkong','xgdz','ltdz','zsgls','njcwjydq','tfdz','shljfsm','njzdxm','jsdgkj','hmgjmy','hzhlbdq','dzspcp','bjqshwykj','bjxcf','bjcg','deqing','zjtx','shyydzsw','fydgs','bjhwmj','szskdsby','bjyllykjyxgs','bjtshssm','shmxmy','bjsyx','jgclbgzpsh','ytsb','Fohc','bjflys','zcfzsy','crjtyxgs','bjlzdl','dffxsh','bjrttz','cdsfy','nbolwl','gzbt','bjqxlj','bjjhxc','bjcdsy','bjsxa','shdpjc','xmdxkj','bjjmxkjc','szsswl','bjyzgmy','lfzy','gzlb','bjcxsd','nbsjjj','xxyzp','szsgyby','bjcpylsm','zlgjbj','fsswsd','bsspzg','gzstypjyxgs','shsmsh','gzlrtpj','jxmbkjxxyxgs','fsshdzkjyxgs','njhqzx','bjyy'"
