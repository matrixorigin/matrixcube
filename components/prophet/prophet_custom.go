package prophet

func (p *defaultProphet) startCustom() {
	if p.cfg.ContainerHeartbeatDataProcessor != nil {
		p.cfg.ContainerHeartbeatDataProcessor.Start(p.storage)
	}
}

func (p *defaultProphet) stopCustom() {
	if p.cfg.ContainerHeartbeatDataProcessor != nil {
		p.cfg.ContainerHeartbeatDataProcessor.Stop(p.storage)
	}
}
