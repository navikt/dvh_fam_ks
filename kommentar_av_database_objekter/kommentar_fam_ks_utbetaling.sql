comment on table dvh_fam_ks.fam_ks_utbetaling is '#NAVN Utbetaling #INNHOLD Tabellen inneholder utbetalingsplan til kontantstøtte.';
comment on column dvh_fam_ks.fam_ks_utbetaling.pk_ks_utbetaling is '#NAVN Primær nøkkel #INNHOLD Primær nøkkel, en unik id. Verdien er kombinasjon av behandlings_id, stonad_fom og stonad_tom.';
comment on column dvh_fam_ks.fam_ks_utbetaling.kafka_offset is '#NAVN Kafka Offset #INNHOLD Kafka offset av kafka topic-en hvor jsonmelding til vedtaket kommer fra.';
comment on column dvh_fam_ks.fam_ks_utbetaling.utbetalt_per_mnd is '#NAVN Utbetalt per måned #INNHOLD Utbetalt månedsbeløp.';
comment on column dvh_fam_ks.fam_ks_utbetaling.stonad_fom is '#NAVN Stønad fom #INNHOLD Stønad fra dato.';
comment on column dvh_fam_ks.fam_ks_utbetaling.stonad_tom is '#NAVN Stønad tom #INNHOLD Stønad til dato.';
comment on column dvh_fam_ks.fam_ks_utbetaling.fk_ks_fagsak is '#NAVN Fremmednøkkel #INNHOLD Fremmednøkkel til FAM_KS_FAGSAK.';
comment on column dvh_fam_ks.fam_ks_utbetaling.hjemmel is '#NAVN Hjemmel #INNHOLD Ikke implementert.';
comment on column dvh_fam_ks.fam_ks_utbetaling.lastet_dato is '#NAVN Lastet dato #INNHOLD Lastet dato når data ble pakket ut av DBT.';