package main

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gomodule/redigo/redis"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/en/siren/protos"
	log "github.com/en/siren/utils/glog"
)

const (
	port = ":50051"
)

var redisPool *redis.Pool
var matchClient pb.MatchClient

// tradingview {time, close, open, high, low, volume}
// coinbase [ time, low, high, open, close, volume ],
type marketInfo struct {
	min        map[int64]pb.KlineInfo
	hour       map[int64]pb.KlineInfo
	day        map[int64]pb.KlineInfo
	last       float64
	update     *list.List
	deals      *list.List // for updating?
	dealsJson  *list.List
	updateTime time.Time
}

var markets map[string]*marketInfo
var lastFlush time.Time

// [1542165471, "ETHBTC", 7, 41, "auth0|5b8f90a24341cb20a04174d0", "auth0|5b8f90a24341cb20a04174d0", "0.03279700", "0.36699287", "0.00012036265157390000", "0.003669928700", 2, 6, "ETH", "BTC"]
type dealsMessage struct {
	time      int64
	market    string
	i         int32  // ?
	j         int32  // ?
	user1     string // ?
	user2     string // ?
	price     string
	amount    string
	fee1      string // ?
	fee2      string // ?
	side      int32
	id        int32
	currency1 string // ?
	currency2 string // ?
}

const (
	KlineMin = iota
	KlineHour
	KlineDay
)

type updateKey struct {
	klineType int
	timestamp int64
}

type server struct{}

func (s *server) MarketStatus(ctx context.Context, in *pb.MarketStatusRequest) (*pb.MarketStatusReply, error) {
	reply := new(pb.MarketStatusReply)
	market := in.GetMarket()
	period := in.GetPeriod()
	m, ok := markets[market]
	if !ok {
		// TODO
		return reply, nil
	}
	now := time.Now()
	start := now.Add(-time.Duration(period) * time.Second).Truncate(time.Minute)
	var info *pb.KlineInfo
	for t := start; t.Before(now); t = t.Add(time.Minute) {
		// fmt.Println("status search", t, t.Unix())
		if kline, ok := m.min[t.Unix()]; ok {
			if info == nil {
				info = new(pb.KlineInfo)
				info.Open = kline.Open
				info.Close = kline.Open
				info.Low = kline.Open
				info.High = kline.Open
			}
			klineInfoMerge(info, &kline)
		}
	}
	if info == nil {
		info = new(pb.KlineInfo)
	}
	reply.Last = m.last
	reply.Open = info.Open
	reply.Close = info.Close
	reply.Low = info.Low
	reply.High = info.High
	reply.Volume = info.Volume
	reply.Deal = info.Deal
	return reply, nil
}

func (s *server) MarketKline(ctx context.Context, in *pb.MarketKlineRequest) (*pb.MarketKlineReply, error) {
	reply := new(pb.MarketKlineReply)
	market := in.GetMarket()
	start := in.GetStart()
	end := in.GetEnd()
	interval := in.GetInterval()
	m, ok := markets[market]
	if !ok {
		// TODO
		return reply, nil
	}

	now := time.Now().Unix()
	if end > now {
		end = now
	}
	if start > end {
		// TODO
		fmt.Println("start > end", start, end)
		return reply, nil
	}
	if interval < 60 {
		// TODO
		fmt.Println("interval < 60", interval)
		return reply, nil
	} else if interval < 3600 {
		if interval%60 != 0 || 3600%interval != 0 {
			// TODO
			fmt.Println("interval:", interval)
			return reply, nil
		}
		getMarketKlineMin(m, start, end, interval, reply)
	} else if interval < 86400 {
		if interval%3600 != 0 || 86400%interval != 0 {
			// TODO
			fmt.Println("interval:", interval)
			return reply, nil
		}
		getMarketKlineHour(m, start, end, interval, reply)
	} else if interval < 86400*7 {
		if interval%86400 != 0 {
			// TODO
			fmt.Println("interval:", interval)
			return reply, nil
		}
		getMarketKlineDay(m, start, end, interval, reply)
	} else if interval == 86400*7 {
		getMarketKlineWeek(m, start, end, interval, reply)
	} else if interval == 86400*30 {
		getMarketKlineMonth(m, start, end, interval, reply)
	} else {
		// TODO
		fmt.Println("interval > 86400 * 30", interval)
		return reply, nil
	}

	return reply, nil
}

func (s *server) MarketDeals(ctx context.Context, in *pb.MarketDealsRequest) (*pb.MarketDealsReply, error) {
	reply := new(pb.MarketDealsReply)
	market := in.GetMarket()
	limit := in.GetLimit()
	lastId := in.GetLastId()
	m, ok := markets[market]
	if !ok {
		// TODO
		return reply, nil
	}
	count := int32(0)
	for e := m.dealsJson.Front(); e != nil; e = e.Next() {
		fmt.Println("dealsJson:", e.Value)
		d := e.Value.(pb.MarketDeal)
		if uint64(d.Id) <= lastId {
			break
		}
		reply.Items = append(reply.Items, &d)
		count++
		if count == limit {
			break
		}
	}
	return reply, nil
}

func (s *server) MarketLast(ctx context.Context, in *pb.MarketLastRequest) (*pb.MarketLastReply, error) {
	reply := new(pb.MarketLastReply)
	market := in.GetMarket()
	m, ok := markets[market]
	if !ok {
		// TODO
		return reply, nil
	}
	reply.Last = m.last
	return reply, nil
}

func (s *server) MarketStatusToday(ctx context.Context, in *pb.MarketStatusTodayRequest) (*pb.MarketStatusTodayReply, error) {
	reply := new(pb.MarketStatusTodayReply)
	market := in.GetMarket()
	info, ok := markets[market]
	if !ok {
		// TODO
		return reply, nil
	}
	now := time.Now()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	entry, ok := info.day[midnight.Unix()]
	if ok {
		fmt.Println("in ok")
		reply.Low = entry.Low
		reply.High = entry.High
		reply.Open = entry.Open
		reply.Last = entry.Close
	} else {
		kline := getLastKline(info.day, midnight.Add(-time.Hour*24), midnight.Add(-time.Hour*24*30), time.Hour*24)
		if kline != nil {
			fmt.Println("in last")
			reply.Low = kline.Close
			reply.High = kline.Close
			reply.Open = kline.Close
			reply.Last = kline.Close
		} else {
			fmt.Println("in else")
			reply.Low = 0.0
			reply.High = 0.0
			reply.Open = 0.0
			reply.Last = 0.0
		}
	}
	start := now.Add(-24 * time.Hour).Truncate(time.Minute)
	volume := 0.0
	for t := start; t.Before(now); t = t.Add(time.Minute) {
		// fmt.Println("volume search", t, t.Unix())
		if kline, ok := info.min[t.Unix()]; ok {
			volume += kline.Volume
		}
	}
	reply.Volume = volume
	// TODO: deal
	fmt.Println("reply:", reply)
	return reply, nil
}

func getLastKline(m map[int64]pb.KlineInfo, start time.Time, end time.Time, interval time.Duration) *pb.KlineInfo {
	fmt.Println("start:", start, "end:", end)
	for i := start; i.After(end); i = i.Add(-interval) {
		// fmt.Println("search", i, i.Unix())
		if kline, ok := m[i.Unix()]; ok {
			return &kline
		}
	}
	return nil
}

func marketExist(market string) bool {
	_, ok := markets[market]
	return ok
}

func loadMarketKlineMin(name string, info *marketInfo) {
	fmt.Println("market name:", name)
	conn := redisPool.Get()
	defer conn.Close()
	key := fmt.Sprintf("k:%s:1m", name)
	s, err := redis.Values(conn.Do("HGETALL", key))
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < len(s); i += 2 {
		// open, close, high, low, volume, deal
		data := s[i+1].([]byte)
		var r []string
		err := json.Unmarshal(data, &r)
		if err != nil {
			fmt.Println("failed to unmarshal redis data", err)
			break
		}
		time, err := strconv.ParseInt(string(s[i].([]byte)), 10, 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}

		open, err := strconv.ParseFloat(r[0], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		close, err := strconv.ParseFloat(r[1], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		high, err := strconv.ParseFloat(r[2], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		low, err := strconv.ParseFloat(r[3], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		volume, err := strconv.ParseFloat(r[4], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		deal, err := strconv.ParseFloat(r[5], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}

		kline := pb.KlineInfo{}
		kline.Time = time
		kline.Close = close
		kline.Open = open
		kline.High = high
		kline.Low = low
		kline.Volume = volume
		kline.Deal = deal
		// fmt.Println("kline data:", kline)
		info.min[time] = kline
	}
}

func loadMarketKlineHour(name string, info *marketInfo) {
	fmt.Println("market name:", name)
	conn := redisPool.Get()
	defer conn.Close()
	key := fmt.Sprintf("k:%s:1h", name)
	s, err := redis.Values(conn.Do("HGETALL", key))
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < len(s); i += 2 {
		// open, close, high, low, volume, deal
		data := s[i+1].([]byte)
		var r []string
		err := json.Unmarshal(data, &r)
		if err != nil {
			fmt.Println("failed to unmarshal redis data", err)
			break
		}
		time, err := strconv.ParseInt(string(s[i].([]byte)), 10, 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}

		open, err := strconv.ParseFloat(r[0], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		close, err := strconv.ParseFloat(r[1], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		high, err := strconv.ParseFloat(r[2], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		low, err := strconv.ParseFloat(r[3], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		volume, err := strconv.ParseFloat(r[4], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		deal, err := strconv.ParseFloat(r[5], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}

		kline := pb.KlineInfo{}
		kline.Time = time
		kline.Close = close
		kline.Open = open
		kline.High = high
		kline.Low = low
		kline.Volume = volume
		kline.Deal = deal
		fmt.Println("kline data:", kline)
		info.hour[time] = kline
	}
}

func loadMarketKlineDay(name string, info *marketInfo) {
	fmt.Println("market name:", name)
	conn := redisPool.Get()
	defer conn.Close()
	key := fmt.Sprintf("k:%s:1d", name)
	s, err := redis.Values(conn.Do("HGETALL", key))
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < len(s); i += 2 {
		// open, close, high, low, volume, deal
		data := s[i+1].([]byte)
		var r []string
		err := json.Unmarshal(data, &r)
		if err != nil {
			fmt.Println("failed to unmarshal redis data", err)
			break
		}
		time, err := strconv.ParseInt(string(s[i].([]byte)), 10, 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}

		open, err := strconv.ParseFloat(r[0], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		close, err := strconv.ParseFloat(r[1], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		high, err := strconv.ParseFloat(r[2], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		low, err := strconv.ParseFloat(r[3], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		volume, err := strconv.ParseFloat(r[4], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}
		deal, err := strconv.ParseFloat(r[5], 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			break
		}

		kline := pb.KlineInfo{}
		kline.Time = time
		kline.Close = close
		kline.Open = open
		kline.High = high
		kline.Low = low
		kline.Volume = volume
		kline.Deal = deal
		fmt.Println("kline data:", kline)
		info.day[time] = kline
	}
}

func loadMarketDeals(name string, info *marketInfo) {
	conn := redisPool.Get()
	defer conn.Close()
	key := fmt.Sprintf("k:%s:deals", name)
	s, err := redis.Values(conn.Do("LRANGE", key, 0, 10000))
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < len(s); i++ {
		data := s[i].([]byte)
		fmt.Println("deals:", string(data))
		var deal pb.MarketDeal
		err := json.Unmarshal(data, &deal)
		if err != nil {
			log.Error().Err(err).Msg("unmarshal market deal")
			continue
		}
		info.dealsJson.PushBack(deal)
	}
}

func loadMarketLast(name string, info *marketInfo) {
	fmt.Println("market name:", name)
	conn := redisPool.Get()
	defer conn.Close()
	key := fmt.Sprintf("k:%s:last", name)
	s, err := redis.String(conn.Do("GET", key))
	fmt.Printf("last:%#v, %v\n", s, err)
	last, err := strconv.ParseFloat(s, 64)
	if err != nil {
		fmt.Println("failed to strconv", err)
		last = float64(0.0)
	}
	info.last = last
}

func klineInfoMerge(info *pb.KlineInfo, update *pb.KlineInfo) {
	info.Close = update.Close
	info.Volume = info.Volume + update.Volume
	info.Deal = info.Deal + update.Deal
	if update.High > info.High {
		info.High = update.High
	}
	if update.Low < info.Low {
		info.Low = update.Low
	}
}

func klineInfoUpdate(info *pb.KlineInfo, price float64, amount float64) {
	info.Close = price
	info.Volume = info.Volume + amount
	info.Deal = info.Deal + price*amount
	if price > info.High {
		info.High = price
	}
	if price < info.Low {
		info.Low = price
	}
}

func getMarketKlineMin(info *marketInfo, start int64, end int64, interval int64, reply *pb.MarketKlineReply) {
	now := time.Now()
	minStart := now.Add(-365 * 24 * time.Hour).Truncate(time.Minute)
	if start < minStart.Unix() {
		start = minStart.Unix()
	}
	tStart := time.Unix(start, 0)
	tInterval := time.Duration(interval) * time.Second
	tStart = tStart.Truncate(tInterval)
	kbefore := getLastKline(info.min, tStart.Add(-60*time.Second), minStart, 60*time.Second)
	klast := kbefore
	step := interval / 60
	fmt.Println("loop begin")
	for s := tStart.Unix(); s <= end; s += interval {
		var kinfo *pb.KlineInfo
		// fmt.Println("inner loop begin")
		for i := int64(0); i < step; i++ {
			timestamp := s + int64(i)*60
			// fmt.Println("get market min search", timestamp)
			if kline, ok := info.min[timestamp]; ok {
				if kinfo == nil {
					kinfo = new(pb.KlineInfo)
					kinfo.Open = kline.Open
					kinfo.Close = kline.Open
					kinfo.Low = kline.Open
					kinfo.High = kline.Open
				}
				klineInfoMerge(kinfo, &kline)

			} else {
				continue
			}
		}
		// fmt.Println("inner loop end")
		if kinfo == nil {
			if klast == nil {
				continue
			}
			kinfo = new(pb.KlineInfo)
			kinfo.Open = klast.Close
			kinfo.Close = klast.Close
			kinfo.Low = klast.Close
			kinfo.High = klast.Close
		}
		kinfo.Time = s
		reply.Items = append(reply.Items, kinfo)
		klast = kinfo
	}
	fmt.Println("loop end")
}

func getMarketKlineHour(info *marketInfo, start int64, end int64, interval int64, reply *pb.MarketKlineReply) {
	now := time.Now()
	minStart := now.Add(-365 * 24 * 10 * time.Hour).Truncate(time.Hour)
	if start < minStart.Unix() {
		start = minStart.Unix()
	}
	tStart := time.Unix(start, 0)
	midnight := time.Date(tStart.Year(), tStart.Month(), tStart.Day(), 0, 0, 0, 0, time.Local)

	for midnight.Unix()+interval <= start {
		midnight = midnight.Add(time.Duration(interval) * time.Second)
	}
	tStart = midnight

	kbefore := getLastKline(info.hour, tStart.Add(-time.Hour), minStart, time.Hour)
	klast := kbefore
	step := interval / 3600
	for s := tStart.Unix(); s <= end; s += interval {
		var kinfo *pb.KlineInfo
		for i := int64(0); i < step; i++ {
			timestamp := s + int64(i)*3600
			if kline, ok := info.hour[timestamp]; ok {
				if kinfo == nil {
					kinfo = new(pb.KlineInfo)
					kinfo.Open = kline.Open
					kinfo.Close = kline.Open
					kinfo.Low = kline.Open
					kinfo.High = kline.Open
				}
				klineInfoMerge(kinfo, &kline)

			} else {
				continue
			}
		}
		if kinfo == nil {
			if klast == nil {
				continue
			}
			kinfo = new(pb.KlineInfo)
			kinfo.Open = klast.Close
			kinfo.Close = klast.Close
			kinfo.Low = klast.Close
			kinfo.High = klast.Close
		}
		kinfo.Time = s
		reply.Items = append(reply.Items, kinfo)
		klast = kinfo
	}
}

func getMarketKlineDay(info *marketInfo, start int64, end int64, interval int64, reply *pb.MarketKlineReply) {
	start = start / interval * interval
	tStart := time.Unix(start, 0)

	kbefore := getLastKline(info.day, tStart.Add(-24*time.Hour), tStart.Add(-30*24*time.Hour), 24*time.Hour)
	klast := kbefore
	step := interval / 86400
	for s := tStart.Unix(); s <= end; s += interval {
		var kinfo *pb.KlineInfo
		for i := int64(0); i < step; i++ {
			timestamp := s + int64(i)*86400
			if kline, ok := info.day[timestamp]; ok {
				if kinfo == nil {
					kinfo = new(pb.KlineInfo)
					kinfo.Open = kline.Open
					kinfo.Close = kline.Open
					kinfo.Low = kline.Open
					kinfo.High = kline.Open
				}
				klineInfoMerge(kinfo, &kline)

			} else {
				continue
			}
		}
		if kinfo == nil {
			if klast == nil {
				continue
			}
			kinfo = new(pb.KlineInfo)
			kinfo.Open = klast.Close
			kinfo.Close = klast.Close
			kinfo.Low = klast.Close
			kinfo.High = klast.Close
		}
		kinfo.Time = s
		reply.Items = append(reply.Items, kinfo)
		klast = kinfo
	}
}

func getMarketKlineWeek(info *marketInfo, start int64, end int64, interval int64, reply *pb.MarketKlineReply) {
	base := start/interval*interval - 3*86400
	for base+interval <= start {
		base += interval
	}
	start = base
	tStart := time.Unix(start, 0)

	kbefore := getLastKline(info.day, tStart.Add(-24*time.Hour), tStart.Add(-30*24*time.Hour), 24*time.Hour)
	klast := kbefore
	step := interval / 86400
	for s := tStart.Unix(); s <= end; s += interval {
		var kinfo *pb.KlineInfo
		for i := int64(0); i < step; i++ {
			timestamp := s + int64(i)*86400
			if kline, ok := info.day[timestamp]; ok {
				if kinfo == nil {
					kinfo = new(pb.KlineInfo)
					kinfo.Open = kline.Open
					kinfo.Close = kline.Open
					kinfo.Low = kline.Open
					kinfo.High = kline.Open
				}
				klineInfoMerge(kinfo, &kline)

			} else {
				continue
			}
		}
		if kinfo == nil {
			if klast == nil {
				continue
			}
			kinfo = new(pb.KlineInfo)
			kinfo.Open = klast.Close
			kinfo.Close = klast.Close
			kinfo.Low = klast.Close
			kinfo.High = klast.Close
		}
		kinfo.Time = s
		reply.Items = append(reply.Items, kinfo)
		klast = kinfo
	}
}

func getMarketKlineMonth(info *marketInfo, start int64, end int64, interval int64, reply *pb.MarketKlineReply) {
	tStart := time.Unix(start, 0)
	monStart := time.Date(tStart.Year(), tStart.Month(), 1, 0, 0, 0, 0, time.Local)

	kbefore := getLastKline(info.day, monStart.Add(-24*time.Hour), tStart.Add(-30*24*time.Hour), 24*time.Hour)
	klast := kbefore
	for s := monStart.Unix(); s <= end; {
		var kinfo *pb.KlineInfo
		year := time.Unix(s, 0).Year()
		next := int(time.Unix(s, 0).Month()) + 1
		var nextMonth time.Month
		if next == 13 {
			nextMonth = time.January
			year = year + 1
		} else {
			nextMonth = time.Month(next)
		}
		monNext := time.Date(year, nextMonth, 1, 0, 0, 0, 0, time.Local)
		for timestamp := s; timestamp < monNext.Unix() && timestamp <= end; timestamp += 86400 {
			if kline, ok := info.day[timestamp]; ok {
				if kinfo == nil {
					kinfo = new(pb.KlineInfo)
					kinfo.Open = kline.Open
					kinfo.Close = kline.Open
					kinfo.Low = kline.Open
					kinfo.High = kline.Open
				}
				klineInfoMerge(kinfo, &kline)
			} else {
				continue
			}
		}
		if kinfo == nil {
			if klast == nil {
				continue
			}
			kinfo = new(pb.KlineInfo)
			kinfo.Open = klast.Close
			kinfo.Close = klast.Close
			kinfo.Low = klast.Close
			kinfo.High = klast.Close
		}
		kinfo.Time = s
		reply.Items = append(reply.Items, kinfo)
		s = monNext.Unix()
		klast = kinfo
	}
}

func marketUpdate(message *dealsMessage) {
	info, ok := markets[message.market]
	if !ok {
		fmt.Println(message.market, "not found")
		return
	}
	price, err := strconv.ParseFloat(message.price, 64)
	if err != nil {
		fmt.Println("failed to strconv", err)
		return
	}
	amount, err := strconv.ParseFloat(message.amount, 64)
	if err != nil {
		fmt.Println("failed to strconv", err)
		return
	}

	// update min
	// discard fraction
	timeMin := time.Unix(0, message.time*1000*1000).Truncate(time.Minute)
	_, ok = info.min[timeMin.Unix()]
	if !ok {
		kinfo := pb.KlineInfo{}
		kinfo.Open = price
		kinfo.Close = price
		kinfo.Low = price
		kinfo.High = price
		info.min[timeMin.Unix()] = kinfo
	}
	kinfo, ok := info.min[timeMin.Unix()]
	if !ok {
		fmt.Println("failed to get kinfo")
		return
	}
	klineInfoUpdate(&kinfo, price, amount)
	info.min[timeMin.Unix()] = kinfo
	info.update.PushBack(updateKey{KlineMin, timeMin.Unix()})

	// update hour
	timeHour := time.Unix(0, message.time*1000*1000).Truncate(time.Hour)
	_, ok = info.hour[timeHour.Unix()]
	if !ok {
		kinfo := pb.KlineInfo{}
		kinfo.Open = price
		kinfo.Close = price
		kinfo.Low = price
		kinfo.High = price
		info.hour[timeHour.Unix()] = kinfo
	}
	kinfo, ok = info.hour[timeHour.Unix()]
	if !ok {
		fmt.Println("failed to get kinfo")
		return
	}
	klineInfoUpdate(&kinfo, price, amount)
	info.hour[timeHour.Unix()] = kinfo
	info.update.PushBack(updateKey{KlineHour, timeHour.Unix()})

	// update day
	t := time.Unix(0, message.time*1000*1000)
	timeDay := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
	_, ok = info.day[timeDay.Unix()]
	if !ok {
		kinfo := pb.KlineInfo{}
		kinfo.Open = price
		kinfo.Close = price
		kinfo.Low = price
		kinfo.High = price
		info.day[timeDay.Unix()] = kinfo
	}
	kinfo, ok = info.day[timeDay.Unix()]
	if !ok {
		fmt.Println("failed to get kinfo")
		return
	}
	klineInfoUpdate(&kinfo, price, amount)
	info.day[timeDay.Unix()] = kinfo
	info.update.PushBack(updateKey{KlineDay, timeDay.Unix()})

	// update last
	info.last = price

	// update deals
	var deal pb.MarketDeal
	deal.Amount = message.amount
	deal.Id = message.id
	deal.Time = message.time
	deal.Price = message.price
	if message.side == 1 {
		deal.Type = "sell"
	} else {
		deal.Type = "buy"
	}
	info.deals.PushBack(deal)
	info.dealsJson.PushFront(deal)

	if info.dealsJson.Len() > 10000 {
		info.dealsJson.Remove(info.dealsJson.Back())
	}

	// set update time
	info.updateTime = time.Now()
}

func flushUpdate(market string, info *marketInfo) {
	conn := redisPool.Get()
	defer conn.Close()
	for e := info.update.Front(); e != nil; e = e.Next() {
		update := e.Value.(updateKey)
		var key string
		var kinfo pb.KlineInfo
		var ok bool
		if update.klineType == KlineMin {
			key = fmt.Sprintf("k:%s:1m", market)
			kinfo, ok = info.min[update.timestamp]
		} else if update.klineType == KlineHour {
			key = fmt.Sprintf("k:%s:1h", market)
			kinfo, ok = info.hour[update.timestamp]
		} else if update.klineType == KlineDay {
			key = fmt.Sprintf("k:%s:1d", market)
			kinfo, ok = info.day[update.timestamp]
		}
		if !ok {
			fmt.Println("kline not found:", update)
			continue
		}
		var r []string
		r = append(r, fmt.Sprintf("%d", kinfo.Open))
		r = append(r, fmt.Sprintf("%d", kinfo.Close))
		r = append(r, fmt.Sprintf("%d", kinfo.High))
		r = append(r, fmt.Sprintf("%d", kinfo.Low))
		r = append(r, fmt.Sprintf("%d", kinfo.Volume))
		r = append(r, fmt.Sprintf("%d", kinfo.Deal))
		b, err := json.Marshal(r)
		if err != nil {
			log.Error().Err(err).Msg("failed to marshal kline")
			continue
		}
		s := string(b)
		fmt.Println("update kline:", s)

		_, err = redis.Int64(conn.Do("HSET", key, update.timestamp, s))
		if err != nil {
			fmt.Println(err)
			continue
		}
		info.update.Remove(e)
	}
}

func flushLast(market string, last float64) {
	conn := redisPool.Get()
	defer conn.Close()
	key := fmt.Sprintf("k:%s:last", market)
	slast := fmt.Sprintf("%.6f", last)
	s, err := redis.String(conn.Do("SET", key, slast))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("update last:", s)
}

func flushDeals(market string, info *marketInfo) {
	conn := redisPool.Get()
	defer conn.Close()
	args := redis.Args{}
	for e := info.deals.Front(); e != nil; e = e.Next() {
		deal := e.Value.(pb.MarketDeal)
		b, err := json.Marshal(deal)
		if err != nil {
			log.Error().Err(err).Msg("failed to marshal deal")
			continue
		}
		s := string(b)
		args = args.Add(s)
	}
	key := fmt.Sprintf("k:%s:deals", market)
	_, err := redis.Int64(conn.Do("LPUSH", key, args))
	if err != nil {
		fmt.Println(err)
	}
	_, err = redis.String(conn.Do("LTRIM", key, 0, 10000))
	if err != nil {
		fmt.Println(err)
	}
	// clear list
	info.deals.Init()
}

func flushMarket() {
	for k, v := range markets {
		if v.updateTime.Before(lastFlush) {
			continue
		}
		// flush update
		flushUpdate(k, v)
		// flush last
		flushLast(k, v.last)
		if v.deals.Len() == 0 {
			continue
		}
		// flush deals
		flushDeals(k, v)
	}
	// flush offset
	lastFlush = time.Now()
}

func clearKline() {
	now := time.Now()

	for _, v := range markets {
		// clear min
		for k := range v.min {
			if k < now.Add(-365*24*time.Hour).Truncate(time.Minute).Unix() {
				delete(v.min, k)
			}
		}
		// clear hour
		for k := range v.hour {
			if k < now.Add(-365*24*10*time.Hour).Truncate(time.Hour).Unix() {
				delete(v.hour, k)
			}
		}
	}
}

func clearRedis() {
	now := time.Now()

	for k := range markets {
		conn := redisPool.Get()
		defer conn.Close()

		key := fmt.Sprintf("k:%s:1m", k)
		s, err := redis.Values(conn.Do("HGETALL", key))
		if err != nil {
			fmt.Println(err)
		}
		for i := 0; i < len(s); i += 2 {
			t, err := strconv.ParseInt(string(s[i].([]byte)), 10, 64)
			if err != nil {
				fmt.Println("failed to strconv", err)
				continue
			}
			if t < now.Add(-365*24*time.Hour).Truncate(time.Minute).Unix() {
				_, err := redis.Values(conn.Do("HDEL", key, string(s[i].([]byte))))
				if err != nil {
					fmt.Println(err)
				}
			}
		}

		key = fmt.Sprintf("k:%s:1h", k)
		s, err = redis.Values(conn.Do("HGETALL", key))
		if err != nil {
			fmt.Println(err)
		}
		for i := 0; i < len(s); i += 2 {
			t, err := strconv.ParseInt(string(s[i].([]byte)), 10, 64)
			if err != nil {
				fmt.Println("failed to strconv", err)
				continue
			}
			if t < now.Add(-365*24*10*time.Hour).Truncate(time.Hour).Unix() {
				_, err := redis.Values(conn.Do("HDEL", key, string(s[i].([]byte))))
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}
}

func main() {
	log.Info().Msg("Starting siren-memorystore")

	viper.SetEnvPrefix("memorystore")
	viper.AutomaticEnv()
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)

	redisHost := viper.GetString("redis-host")
	redisPort := viper.GetString("redis-port")
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)

	const maxConnections = 10
	redisPool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", redisAddr)
	}, maxConnections)

	{
		conn := redisPool.Get()
		defer conn.Close()

		s, err := redis.String(conn.Do("PING"))
		fmt.Printf("%#v, %v\n", s, err)
	}

	{
		conn, err := grpc.Dial("siren-match:50051", grpc.WithInsecure())
		if err != nil {
			log.Fatal().Err(err).Msg("did not connect")
		}
		defer conn.Close()
		matchClient = pb.NewMatchClient(conn)
		log.Info().Msg("connected to siren-match:50051")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := matchClient.GetSymbols(ctx, &pb.EmptyRequest{})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get currency pairs")
	}

	markets = make(map[string]*marketInfo)
	for _, cp := range r.Symbols {
		market := cp.BaseCurrency + cp.QuoteCurrency
		markets[market] = new(marketInfo)
		markets[market].update = list.New()
		markets[market].deals = list.New()
		markets[market].dealsJson = list.New()
		markets[market].min = make(map[int64]pb.KlineInfo)
		markets[market].hour = make(map[int64]pb.KlineInfo)
		markets[market].day = make(map[int64]pb.KlineInfo)
		loadMarketKlineMin(market, markets[market])
		loadMarketKlineHour(market, markets[market])
		loadMarketKlineDay(market, markets[market])
		loadMarketDeals(market, markets[market])
		loadMarketLast(market, markets[market])
	}
	// for k, v := range markets {
	// 	fmt.Println("market:", k)
	// 	fmt.Println("min>")
	// 	for k1, v1 := range v.min {
	// 		fmt.Println(k1, v1)
	// 	}
	// 	fmt.Println("hour>")
	// 	for k1, v1 := range v.hour {
	// 		fmt.Println(k1, v1)
	// 	}
	// 	fmt.Println("day>")
	// 	for k1, v1 := range v.day {
	// 		fmt.Println(k1, v1)
	// 	}
	// }

	var offset int64
	{
		conn := redisPool.Get()
		defer conn.Close()

		s, err := redis.String(conn.Do("GET", "k:offset"))
		fmt.Printf("offset:%#v, %v\n", s, err)
		offset, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			fmt.Println("failed to strconv", err)
			offset = int64(0)
		}
	}
	fmt.Println("offset:", offset)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	topic := "deals"
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       "REMOVED.us-central1.gcp.confluent.cloud:9092",
		"api.version.request":     true,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"sasl.mechanisms":         "PLAIN",
		"security.protocol":       "SASL_SSL",
		"ssl.ca.location":         "/etc/ssl/cert.pem",
		"sasl.username":           "REMOVED",
		"sasl.password":           "REMOVED",
		"group.id":                "siren-memorystore",
		"enable.partition.eof":    true,
		"auto.offset.reset":       "earliest"})

	if err != nil {
		panic(err)
	}

	err = c.Subscribe(topic, nil)
	if err != nil {
		panic(err)
	}

	go func() {
		run := true

		for run == true {
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				run = false
			default:
				ev := c.Poll(100)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					fmt.Printf("%% Message on %s:\n%s\n",
						e.TopicPartition, string(e.Value))
					if e.Headers != nil {
						fmt.Printf("%% Headers: %v\n", e.Headers)
					}
					var dm dealsMessage
					tmp := []interface{}{&dm.time, &dm.market, &dm.i, &dm.j, &dm.user1, &dm.user2, &dm.price, &dm.amount, &dm.fee1, &dm.fee2, &dm.side, &dm.id, &dm.currency1, &dm.currency2}
					err := json.Unmarshal(e.Value, &tmp)
					if err != nil {
						fmt.Fprintf(os.Stderr, "%% Unmarshal Error: %v\n", err)
					}
					marketUpdate(&dm)
					// TODO: update offset?
				case kafka.PartitionEOF:
					fmt.Printf("%% Reached %v\n", e)
				case kafka.Error:
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
					run = false
				default:
					fmt.Printf("Ignored %v\n", e)
				}
			}
		}

		fmt.Printf("Closing consumer\n")
		c.Close()
	}()

	go func() {
		marketTimer := time.After(10 * time.Second) // 10 sec
		clearTimer := time.After(time.Hour)         // 1 hour
		redisTimer := time.After(24 * time.Hour)    // 1 day
		for {
			select {
			case <-marketTimer:
				fmt.Println("in market timer ...")
				flushMarket()
				marketTimer = time.After(10 * time.Second)
			case <-clearTimer:
				fmt.Println("in clear timer ...")
				clearKline()
				clearTimer = time.After(time.Hour)
			case <-redisTimer:
				fmt.Println("in redis timer ...")
				go clearRedis()
				redisTimer = time.After(24 * time.Hour)
			}
		}
	}()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}
	s := grpc.NewServer()
	// TODO
	pb.RegisterMemorystoreServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatal().Err(err).Msg("failed to serve")
	}
}
