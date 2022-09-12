use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let r = self
            .keys
            .iter()
            .map(|k| {
                store.get(&self.table, k).map(|v| Kvpair {
                    key: k.into(),
                    value: v,
                })
            })
            .collect::<Result<Vec<Kvpair>, KvError>>();

        r.into()
    }
}

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let table = &self.table;
        let r = self
            .pairs
            .into_iter()
            .map(|pair| {
                let p = pair.clone();
                store.set(table, p.key, p.value.unwrap()).map(|v| Kvpair {
                    key: pair.key,
                    value: v,
                })
            })
            .collect::<Result<Vec<Kvpair>, KvError>>();

        match r {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let r = self
            .keys
            .iter()
            .map(|k| {
                store.del(&self.table, k).map(|v| Kvpair {
                    key: k.into(),
                    value: v,
                })
            })
            .collect::<Result<Vec<Kvpair>, KvError>>();

        r.into()
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(_)) => Value::from(true).into(),
            Ok(None) => Value::from(false).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let r = self
            .keys
            .iter()
            .map(|k| {
                store.get(&self.table, k).map(|v| Kvpair {
                    key: k.clone(),
                    value: v,
                })
            })
            .collect::<Result<Vec<Kvpair>, KvError>>();

        r.into()
    }
}

#[cfg(test)]
mod tests {
    use http::Request;

    use super::*;
    use crate::{command_request::RequestData, memory::MemTable};

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hdel("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_not_found(res);

        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hdel("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);

        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_not_found(res);
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hexist("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[false.into()], &[]);

        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hexist("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[true.into()], &[]);
    }

    #[test]
    fn hmexit_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hmexist(
            "score",
            vec!["u1".to_string(), "u2".to_string(), "u3".to_string()],
        );
        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::empty_value("u1"),
                Kvpair::empty_value("u2"),
                Kvpair::empty_value("u3"),
            ],
        );

        let cmds = vec![
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u1", 6.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hmexist(
            "score",
            vec!["u1".to_string(), "u2".to_string(), "u3".to_string()],
        );
        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("u1", 6.into()),
                Kvpair::new("u2", 8.into()),
                Kvpair::empty_value("u3"),
            ],
        );
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_not_found(res);
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
            CommandRequest::new_hset("score", "u1", 6.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hgetall("score");
        let res = dispatch(cmd, &store);
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs);
    }

    #[test]
    fn hmget_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
            CommandRequest::new_hset("score", "u1", 6.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hmget(
            "score",
            vec!["u1".to_string(), "u2".to_string(), "u3".to_string()],
        );
        let res = dispatch(cmd, &store);
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs);
    }

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hmdel(
            "score",
            vec!["u1".to_string(), "u2".to_string(), "u3".to_string()],
        );
        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::empty_value("u1"),
                Kvpair::empty_value("u2"),
                Kvpair::empty_value("u3"),
            ],
        );

        let cmd = CommandRequest::new_hmset(
            "score",
            vec![
                Kvpair::new("u1", 6.into()),
                Kvpair::new("u2", 8.into()),
                Kvpair::new("u3", 11.into()),
            ],
        );
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hmdel(
            "score",
            vec!["u1".to_string(), "u2".to_string(), "u3".to_string()],
        );
        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("u1", 6.into()),
                Kvpair::new("u2", 8.into()),
                Kvpair::new("u3", 11.into()),
            ],
        );
    }

    #[test]
    fn hmset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hmset(
            "score",
            vec![
                Kvpair::new("u1", 6.into()),
                Kvpair::new("u2", 8.into()),
                Kvpair::new("u3", 11.into()),
            ],
        );
        let res = dispatch(cmd, &store);
        let pairs = &[
            Kvpair::empty_value("u1"),
            Kvpair::empty_value("u2"),
            Kvpair::empty_value("u3"),
        ];
        assert_res_ok(res, &[], pairs);

        let cmd = CommandRequest::new_hmset(
            "score",
            vec![
                Kvpair::new("u1", 12.into()),
                Kvpair::new("u2", 13.into()),
                Kvpair::new("u3", 15.into()),
            ],
        );
        let res = dispatch(cmd, &store);
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs);
    }

    fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
        match cmd.request_data.unwrap() {
            RequestData::Hget(v) => v.execute(store),
            RequestData::Hgetall(v) => v.execute(store),
            RequestData::Hset(v) => v.execute(store),
            RequestData::Hmget(v) => v.execute(store),
            RequestData::Hmset(v) => v.execute(store),
            RequestData::Hdel(v) => v.execute(store),
            RequestData::Hmdel(v) => v.execute(store),
            RequestData::Hexist(v) => v.execute(store),
            RequestData::Hmexist(v) => v.execute(store),
            _ => todo!(),
        }
    }

    // 测试成功返回的结果
    fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
        res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(res.status, 200);
        assert_eq!(res.message, "");
        assert_eq!(res.values, values);
        assert_eq!(res.pairs, pairs);
    }

    // 测试失败返回的结果
    fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
        assert_eq!(res.status, code);
        assert!(res.message.contains(msg));
        assert_eq!(res.values, &[]);
        assert_eq!(res.pairs, &[]);
    }

    // 测试404 not found
    fn assert_res_not_found(res: CommandResponse) {
        assert_res_error(res, 404, "Not found");
    }
}
