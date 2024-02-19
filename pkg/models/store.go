// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/gocql/gocql"
)

func init() {
	if filepath.Separator != '/' {
		log.Panicf("bad Separator = '%c', must be '/'", filepath.Separator)
	}
}

const JodisDir = "/jodis"

func JodisPath(product string, token string) string {
	return filepath.Join(JodisDir, product, fmt.Sprintf("proxy-%s", token))
}

const CodisDir = "/codis3"

func ProductDir(product string) string {
	return filepath.Join(CodisDir, product)
}

func LockPath(product string) string {
	return filepath.Join(product, "topom")
}

func SlotPath(product string, sid int) string {
	return filepath.Join(CodisDir, product, "slots", fmt.Sprintf("slot-%04d", sid))
}

func GroupDir(product string) string {
	return filepath.Join(CodisDir, product, "group")
}

func ProxyDir(product string) string {
	return filepath.Join(CodisDir, product, "proxy")
}

func GroupPath(product string, gid int) string {
	return filepath.Join(CodisDir, product, "group", fmt.Sprintf("group-%04d", gid))
}

func ProxyPath(product string, token string) string {
	return filepath.Join(product, "proxy", token)
}

func SentinelPath(product string) string {
	return filepath.Join(CodisDir, product, "sentinel")
}

func LoadTopom(client Client, product string, must bool) (*Topom, error) {
	b, err := client.Read(LockPath(product), must)
	if err != nil || b == nil {
		return nil, err
	}
	t := &Topom{}
	if err := jsonDecode(t, b); err != nil {
		return nil, err
	}
	return t, nil
}

type NodeGroup struct {
	id      uint
	members []uint
}

// Redis cluster topology
type RedisCluster struct {
	ips    []string
	groups map[uint]*NodeGroup
}

func (c *RedisCluster) NumGroupSlots() int {
	ng_count := len(c.groups)
	return (MaxSlotNum + ng_count) / ng_count
}

func (c *RedisCluster) SlotMap(sid int) int {
	return sid / c.NumGroupSlots()
}

func (c *RedisCluster) Group(gid uint) []*GroupServer {
	ng := c.groups[gid]
	servers := make([]*GroupServer, 0, 3)
	for _, mi := range ng.members {
		servers = append(servers, &GroupServer{Addr: fmt.Sprintf("%s:6379", c.ips[mi])})
	}
	return servers
}

func (c *RedisCluster) Groups() map[int]*Group {
	groups := make(map[int]*Group, len(c.groups))
	for gid := range c.groups {
		groups[int(gid)] = &Group{
			Id:      int(gid),
			Servers: c.Group(gid),
		}
	}
	return groups
}

func LoadRedisCluster(session *gocql.Session) (*RedisCluster, error) {
	var ips []string
	var ng_members []string
	var ngids []uint
	if err := session.Query("SELECT ips,ng_members,ngids FROM cluster_config").Scan(&ips, &ng_members, &ngids); err != nil {
		log.Errorf("request cluster_config failed: %v", err)
		return nil, err
	}
	if len(ng_members) != len(ngids) {
		log.Panicf("len(ng_members)[%d] != len(ngids)[%d]", len(ng_members), len(ngids))
	}
	groups := make(map[uint]*NodeGroup, len(ngids))
	for i := range ng_members {
		members := make([]uint, 0, 3)
		for _, m := range strings.Split(ng_members[i], " ") {
			mi, err := strconv.Atoi(m)
			if err != nil {
				log.Panic(err)
			}
			members = append(members, uint(mi))
		}
		ngid := ngids[i]
		groups[ngid] = &NodeGroup{
			id:      ngid,
			members: members,
		}
	}
	return &RedisCluster{ips, groups}, nil
}

type Store struct {
	session *gocql.Session
	cluster *RedisCluster
	client  Client
	product string
}

func NewStore(session *gocql.Session, client Client, product string) *Store {
	cluster, err := LoadRedisCluster(session)
	if err != nil {
		return nil
	}
	return &Store{session, cluster, client, product}
}

func (s *Store) Close() error {
	s.session.Close()
	return s.client.Close()
}

func (s *Store) Client() Client {
	return s.client
}

func (s *Store) LockPath() string {
	return LockPath(s.product)
}

func (s *Store) SlotPath(sid int) string {
	return SlotPath(s.product, sid)
}

func (s *Store) GroupDir() string {
	return GroupDir(s.product)
}

func (s *Store) ProxyDir() string {
	return ProxyDir(s.product)
}

func (s *Store) GroupPath(gid int) string {
	return GroupPath(s.product, gid)
}

func (s *Store) ProxyPath(token string) string {
	return ProxyPath(s.product, token)
}

func (s *Store) SentinelPath() string {
	return SentinelPath(s.product)
}

const (
	CQL_INSERT = "INSERT INTO codis (product,directory,file,content) VALUES(?,?,?,?)"
	CQL_DELETE = "DELETE FROM codis WHERE product=? AND directory=? AND file=?"
	CQL_SELECT = "SELECT content FROM codis WHERE product=? AND directory=? AND file=?"
	CQL_SCAN   = "SELECT file,content FROM codis WHERE product=? AND directory=?"

	DIR_HOME  = "~"
	DIR_PROXY = "proxy"
)

func (s *Store) Acquire(topom *Topom) error {
	return s.session.Query(CQL_INSERT, s.product, DIR_HOME, "topom", topom.Encode()).Exec()
}

func (s *Store) Release() error {
	return s.session.Query(CQL_DELETE, s.product, DIR_HOME, "topom").Exec()
}

func (s *Store) LoadTopom(must bool) (*Topom, error) {
	var b []byte
	err := s.session.Query(CQL_SELECT, s.product, DIR_HOME, "topom").Scan(&b)
	if err != nil {
		return nil, err
	}
	t := &Topom{}
	if err := jsonDecode(t, b); err != nil {
		return nil, err
	}
	return t, nil
}

func (s *Store) SlotMappings() ([]*SlotMapping, error) {
	slots := make([]*SlotMapping, MaxSlotNum)
	for i := range slots {
		m, err := s.LoadSlotMapping(i, false)
		if err != nil {
			return nil, err
		}
		if m != nil {
			slots[i] = m
		} else {
			slots[i] = &SlotMapping{Id: i}
		}
	}
	return slots, nil
}

func (s *Store) LoadSlotMapping(sid int, must bool) (*SlotMapping, error) {
	m := &SlotMapping{
		Id:      sid,
		GroupId: s.cluster.SlotMap(sid),
	}
	return m, nil
}

func (s *Store) UpdateSlotMapping(m *SlotMapping) error {
	return s.client.Update(s.SlotPath(m.Id), m.Encode())
}

func (s *Store) ListGroup() (map[int]*Group, error) {
	return s.cluster.Groups(), nil
}

func (s *Store) LoadGroup(gid int, must bool) (*Group, error) {
	g := &Group{
		Id:      gid,
		Servers: s.cluster.Group(uint(gid)),
	}
	return g, nil
}

func (s *Store) UpdateGroup(g *Group) error {
	return s.client.Update(s.GroupPath(g.Id), g.Encode())
}

func (s *Store) DeleteGroup(gid int) error {
	return s.client.Delete(s.GroupPath(gid))
}

func (s *Store) ListProxy() (map[string]*Proxy, error) {
	proxy := make(map[string]*Proxy)
	scanner := s.session.Query("SELECT content FROM codis WHERE product=? AND directory=?", s.product, DIR_PROXY).Iter().Scanner()
	var config []byte
	for scanner.Next() {
		err := scanner.Scan(&config)
		if err != nil {
			return nil, err
		}
		p := &Proxy{}
		if err := jsonDecode(p, config); err != nil {
			return nil, err
		}
		proxy[p.Token] = p
	}
	return proxy, nil
}

func (s *Store) LoadProxy(token string, must bool) (*Proxy, error) {
	var config []byte
	err := s.session.Query(CQL_SELECT, s.product, DIR_PROXY, token).Scan(&config)
	if err != nil || config == nil {
		return nil, err
	}
	p := &Proxy{}
	if err := jsonDecode(p, config); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) UpdateProxy(p *Proxy) error {
	return s.session.Query(CQL_INSERT, s.product, DIR_PROXY, p.Token, p.Encode()).Exec()
}

func (s *Store) DeleteProxy(token string) error {
	return s.session.Query(CQL_DELETE, s.product, DIR_PROXY, token).Exec()
}

func (s *Store) LoadSentinel(must bool) (*Sentinel, error) {
	b, err := s.client.Read(s.SentinelPath(), must)
	if err != nil || b == nil {
		return nil, err
	}
	p := &Sentinel{}
	if err := jsonDecode(p, b); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) UpdateSentinel(p *Sentinel) error {
	return s.client.Update(s.SentinelPath(), p.Encode())
}

func ValidateProduct(name string) error {
	if regexp.MustCompile(`^\w[\w\.\-]*$`).MatchString(name) {
		return nil
	}
	return errors.Errorf("bad product name = %s", name)
}
