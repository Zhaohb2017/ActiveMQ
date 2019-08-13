#!/usr/bin/python
# -*- coding: UTF-8 -*-

import stomp
from db import *
from log import Logger
from db import DB
import json
import  time
import io
import yaml

conf_path = "./conf.yaml"
def get_conf_data():
    _file_data = io.open(conf_path, 'r', encoding='utf-8')
    conf_data = yaml.load(_file_data)
    _file_data.close()
    return conf_data


conf_data = get_conf_data()
debug_file_name = '%s.debug.log' % (time.strftime("%Y%m%d"))
debug_log = Logger(debug_file_name)

# 数值转换
def msg_transform(data):
    if len(data) is 0:
        return debug_log.logger.debug("insert data is empty")

    else:
        #转换字典
        if type(data) is not dict:
            msg_json = json.loads(data)
            return msg_json
        elif type(data) is dict:
            return  data

#时间戳转换
def time_stamp_conversion(data,type='y_m_d'):
    timeStamp =data
    timeArray = time.localtime(timeStamp)
    if type != "h_m_s":
        otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
        return otherStyleTime
    else:
        otherStyleTime = time.strftime("%H%M%S", timeArray)
        return otherStyleTime

#找出大赢家
def winner_data(message):
    all_score = message["room_players"]
    ret = max(all_score, key=lambda dic: dic['roomscore'])
    winners = []
    for room_players in all_score:
        for k, v in room_players.items():
            if k == "roomscore":
                if v == ret['roomscore']:
                    winners.append(room_players)
    return winners


#数据结构解析
def data_structure_analysis(message):
    db = DB(conf_data["DB"])

    #-------------------------------------
    club_id = message["club_id"]  # 俱乐部会长ID
    club_master = message["club_master"]  # 俱乐部会长ID
    db_sign = int(club_master) % 10  # 数据表后缀
    date = message['create_time']
    create_time = time_stamp_conversion(date)  # 房间创建时间
    paytimes = message["now_time"]
    new_times = time_stamp_conversion(date, type="h_m_s")  # 最新更新时间
    players = message["room_players"]
    payment_user = ''  #支付玩家
    payment_user_money = ''  #支付玩家money
    deduction_room  = None   #是否扣费房间 是 turn ,否 False
    room_owner = message["room_owner"]  #房间房主ID
    isfinished = None         #有无效房间区分
    room_type= int(message["pay_params"]["pay_type"])  #房卡还是点券房
    user_money = {}        #大局结算支付点券玩家
    #---------------------------------------

    if len(players) is 0:
        return  debug_log.logger.debug("players is null")

    else:

        try:
            isfinished = int(message["isfinished"])  #有效无效区分
        except Exception as e:
            debug_log.logger.debug(e)

        #找出支付玩家ID
        dict_params = message["pay_params"]['params']
        if room_type is 0: #房卡
            for k,v in dict_params.items():
                if k == "payreal": #支付金额
                    if v is not 0:
                        payment_user = dict_params["payplayer"]
                        payment_user_money = v
                        deduction_room = True
                    elif v == dict_params["roomcost"]:   # 假设需要支付的金额为零
                        payment_user = dict_params["payplayer"]
                        payment_user_money = v
                        deduction_room = True
                    else: #支付金额为0
                        payment_user = 0
                        payment_user_money = 0
                        deduction_room = False
        elif room_type is 1: #点券
            for k,v in dict_params.items():
                if k == "roomcost": #需要支付的点券
                    if v is not 0:
                        for i in message['pay_params']["params"]['payinfo']:
                            if i["palyer_rebate_pay"] >= 0:
                                user_money[i['mid']] = i["palyer_rebate_pay"]  #支付点券的玩家的钱
                                deduction_room = True
        #找出大赢家
        winner_ID = [] #大赢家ID
        WinnerScore = None   #大赢家分数
        _winner = winner_data(message)
        for w in _winner:
            for k,v in w.items():
                if k =="mid":
                    winner_ID.append(v)
                if k =="roomscore":
                    WinnerScore = v


        for user_dict in players:
            mid = ''
            club_id = message["club_id"]  # 俱乐部ID
            date = message['create_time']
            create_time = time_stamp_conversion(date,)  # 房间创建时间
            paytimes = message["now_time"]
            new_times = time_stamp_conversion(paytimes, type="h_m_s")  # 最新更新时间
            playtotal = 0   # 创建房间数(扣费计算)
            croom = 0           # 创建房间数
            playcount = message["played_ju"]  # 玩的小局数
            jointotal = 0  # '用户参与房间数（扣费即算）'
            joinroom = 0  # 用户参与房间数
            integral = 0  # 胡牌积分数
            winner = 0  # 大赢家次数
            integraltotal = 0  # 胡牌积分数（扣费即算）
            winnertotal = 0  # 大赢家次数（扣费即算）
            singlemaxscore = 0  # '单大局最大积分（所有房间）'
            singlemaxscoretotal = 0  # '单大局最大积分（扣费即算）
            winnerscore = 0  # 大赢家积分  有效房间
            winnertotalscore = 0  # 大赢家积分  非有效房间
            colltype = 3  # 游戏类型 1跑胡子 2打筒子 3跑得快  4麻将'
            paytotal = 0  # 消耗点券/金币数（扣费即算）
            payreal = 0  # 消耗点券/金币数（按实际收费统计，点券欠费时只计算收到的实际值）
            paytotal_valid = 0  # 消耗点券/金币数（有效牌局--实际消耗）
            payreal_valid = 0  # 消耗点券/金币数（有效牌局--实际消耗减欠费点券）


            for k, v in user_dict.items():
                if k == 'mid': # room_players玩家的MID
                    mid = v  # mid

                    #点券：
                    if room_type is 1:
                        for u,m in user_money.items():
                            if mid == u:
                                if deduction_room is True:  # 扣费成功
                                    paytotal = m  # (消耗点券（扣费即算)
                    #房卡
                    if room_type is 0:
                        if mid == payment_user:  # 找出支付玩家
                            if deduction_room is True:  # 扣费成功
                                pass
                                # paytotal = payment_user_money  # (消耗点券/金币数（扣费即算)



                    if int(mid) == int(room_owner): #房主ID
                        if isfinished is 1:
                            croom += 1  # 创建房间数

                        if deduction_room is True:  #扣费成功
                            playtotal += 1 #创建房间数（扣费即算）

                            # paytotal += payment_user_money  #(消耗点券/金币数（扣费即算)
                            #singlemaxscoretotal = WinnerScore  #'单大局最大积分（扣费即算）

                    if deduction_room is True:
                        jointotal +=1               #用户参与房间(扣费)

                    #-----------------------有效房间参数-------------------------
                    if  isfinished is 1:  #-----------有效房间

                        joinroom += 1  # 用户参与房间

                    #----------------------------------------------
                    for winner_user in winner_ID:
                        if int(mid) == int(winner_user):  #大赢家ID
                            if deduction_room is True:  # 扣费成功
                                winnertotal += 1  # 大赢家次数(扣费)

                            if isfinished is 1:  #有效房间
                                winner += 1  # 大赢家次数
                                winnerscore += WinnerScore #'大赢家积分  有效房间'
                                winnertotalscore += WinnerScore   #'大赢家积分  非有效房间'

                            elif isfinished is 0:  #无效房间
                                winnertotalscore += WinnerScore   #'大赢家积分  非有效房间'
                    #-----------------------点券房-------------------------
                    if room_type is 1:
                        for j in message['pay_params']["params"]['payinfo']:
                            for _key,_val in j.items():
                                if _key == "mid":
                                    if _val == v:
                                        palyer_rebate_pay = int(j["palyer_rebate_pay"])  # 个人消耗
                                        owe_card = int(j["owe_card"])  # 欠费
                                        if isfinished is 1:  # 有效房间
                                            # 欠费情况
                                            if owe_card >= 0:
                                                payreal_valid = palyer_rebate_pay - owe_card  # 消耗点券/金币数（有效牌局--实际消耗减欠费点券）
                                                paytotal_valid = palyer_rebate_pay  # 消耗点券/金币数（有效牌局--实际消耗
                                                payreal = palyer_rebate_pay - owe_card  # '消耗点券/金币数（按实际收费统计，点券欠费时只计算收到的实际值）'
                                            elif owe_card < 0:
                                                payreal = 0   #实际支付0
                                                payreal_valid = 0 #实际支付为零
                                                paytotal_valid = abs(owe_card)
                                                paytotal = abs(owe_card)

                                        elif isfinished is 0:  # 无效房间
                                            payreal = palyer_rebate_pay - owe_card  # '消耗点券/金币数（按实际收费统计，点券欠费时只计算收到的实际值）'


                elif k == "roomscore":  ##胡牌积分数
                    # -----------------------有效房间参数-------------------------
                    if isfinished is 1:  # -----------有效房间
                        integral = v

                    if deduction_room is True:
                        integraltotal += v      #胡牌积分数(扣费)


            sql = '''
                INSERT INTO majiangcs_log_mq.club_user_day3_{db_sign} (
            	`mid`,
            	`clubid`,
            	`date`,
            	`paytimes`,
            	`playtotal`,
            	`croom`,
            	`playcount`,
            	`playtime`,
            	`jointotal`,
            	`joinroom`,
            	`integral`,
            	`winner`,
            	`colltype`,
            	`room_type`,
            	`integraltotal`,
            	`winnertotal`,
            	`singlemaxscore`,
            	`singlemaxscoretotal`,
            	`winnerscore`,
            	`winnertotalscore`,
            	`paytotal`,
            	`payreal`,
            	`paytotal_valid`,
            	`payreal_valid`
            )
            VALUES
            	(
            		{mid},
            		{clubid},
            		{_time},
            		{paytimes},
            		{playtotal},
            		{croom},
            		{playcount},
            		{playtime},
            		{jointotal},
            		{joinroom},
            		{integral},
            		{winner},
            		{colltype},
            		{room_type},
            		{integraltotal},
            		{winnertotal},
            		{singlemaxscore},
            		{singlemaxscoretotal},
            		{winnerscore},
            		{winnertotalscore},
            		{paytotal},
            		{payreal},
            		{paytotal_valid},
            		{payreal_valid}
            	) ON DUPLICATE KEY UPDATE `playcount` = `playcount` + {add_playcount},
            	`playtotal` =`playtotal` + {add_playtotal},	
            	`joinroom` = `joinroom` + {add_joinroom},
            	`croom` = `croom` + {add_croom},
            	`integral` = `integral` + {add_integral},
            	`winner` = `winner` + {add_winner},
            	`integraltotal` = `integraltotal` +{add_integraltotal},
            	`winnertotal` = `winnertotal` + {add_winnertotal},
            	`winnerscore` = `winnerscore` + {add_winnerscore},
            	`winnertotalscore` = `winnertotalscore` + {add_winnertotalscore},
            	`paytotal` = `paytotal` + {add_paytotal},
            	`payreal` = `payreal` + {add_payreal},
            	`paytotal_valid` = `paytotal_valid` + {add_paytotal_valid},
            	`payreal_valid` = `payreal_valid` +{add_payreal_valid},
            	`jointotal` = `jointotal` + {add_jointotal};

                '''.format(
                db_sign=db_sign,
                mid=mid,
                _time="'%s'"%create_time,
                clubid=club_id,
                paytimes=new_times,
                playtotal=playtotal,
                croom=croom,
                playcount=playcount,
                playtime=0,
                jointotal=jointotal,
                joinroom=joinroom,
                integral=integral,
                winner=winner,
                colltype=colltype,
                room_type=room_type,
                integraltotal=integraltotal,
                winnertotal=winnertotal,
                singlemaxscore=singlemaxscore,
                singlemaxscoretotal=singlemaxscoretotal,
                winnerscore=winnerscore,
                winnertotalscore=winnertotalscore,
                paytotal=paytotal,
                payreal=payreal,
                paytotal_valid=paytotal_valid,
                payreal_valid=payreal_valid,
                add_playcount=playcount,
                add_playtotal = playtotal,
                add_croom = croom,
                add_jointotal = jointotal,
                add_joinroom = joinroom,
                add_integral = integral,
                add_winner = winner,
                add_integraltotal = integraltotal,
                add_winnertotal = winnertotal,
                add_winnerscore = winnerscore,
                add_winnertotalscore = winnertotalscore,
                add_paytotal = paytotal,
                add_payreal = payreal,
                add_paytotal_valid = paytotal_valid,
                add_payreal_valid = payreal_valid)

            debug_log.logger.debug("insert sql :%s"%sql)
            db.updateMulti(sql)




class SampleListener(object):
    def on_message(self,headers, message):
        debug_log.logger.debug('headers: %s' % headers['destination'])
        debug_log.logger.debug('headers: {0}'.format(headers))
        debug_log.logger.debug('message: %s\n' % message)
        json_msg = msg_transform(message)
        data_structure_analysis(message=json_msg)



## 从主题接收消息
def receive_from_topic():
    conn = stomp.Connection10([(conf_data["mq"]["host"], conf_data["mq"]["port"])])
    conn.set_listener(conf_data["conlistener_name"], SampleListener())
    conn.start()
    # conn.connect(__user, __password, wait=True)
    conn.connect(wait=True)
    conn.subscribe(conf_data["queue_Xlogger"])
    # conn.subscribe(__topic_name2)
    while True:
        pass
    conn.disconnect()

if __name__ == '__main__':
    receive_from_topic()


