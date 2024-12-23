import { Injectable, Res } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In, Brackets } from 'typeorm';
import { CommunicationEvents } from '../entity/communication-events.entity';
import { AvailabilityEvents } from '../entity/availability-events.entity';
import { Nodes } from 'src/modules/nodes/entity/nodes.entity';
import {
  CommunicationMethod,
  MessageType,
  NetworkStatus,
  SignalClass,
  NodeType,
  Event_Key,
} from 'src/constants';
import { mergeResults } from 'src/util/mergeResultsEventSummary';
import { Cron } from '@nestjs/schedule';
import { NodeService } from 'src/modules/nodes/service/nodes.service';
import { isValidHeader } from 'src/util/isValidFileImport';
import { getEnumValue } from 'src/util/handleEnumValue';
import * as moment from 'moment-timezone';
import e, { Response } from 'express';
import * as fs from 'fs-extra';
import * as fastcsv from 'fast-csv';
import { exportDataToZip } from 'src/util/exportData';
import { IgnoreEventsService } from 'src/modules/users/service/ignore-events.service';
import { HttpHelper } from '@util/http';
import {
  convertUnixToFormat,
  convertTimeZone,
  convertToUTC,
  randomChoice,
  randomNumber,
} from '@util/function';
import { GatewayService } from 'src/modules/gateway/service/gateway.service';
import { ScannerEvents } from '../entity/scanner-events';
import { IScannerEvent } from '@interface/event.interface';

@Injectable()
export class EventsService {
  constructor(
    @InjectRepository(CommunicationEvents)
    private communicationEventsRepository: Repository<CommunicationEvents>,
    @InjectRepository(AvailabilityEvents)
    private availabilityEventsRepository: Repository<AvailabilityEvents>,
    @InjectRepository(ScannerEvents)
    private scannerEventRepository: Repository<ScannerEvents>,
    @InjectRepository(Nodes)
    private NodesRepo: Repository<Nodes>,
    private nodeService: NodeService,
    private ignoreEventsService: IgnoreEventsService,
  ) {}

  async saveEvent(type: number, events: any) {
    try {
      if (type === 1) {
        return this.availabilityEventsRepository.save(events);
      } else if (type === 2) {
        return this.communicationEventsRepository.save(events);
      } else if (type === 3) {
        return this.scannerEventRepository.save(events);
      }
    } catch (err) {
      console.error(err);
    }
  }

  async deleteEvent(type: number, eventIds: string[], deleteAll: boolean) {
    try {
      const repositoryMapping = {
        1: this.availabilityEventsRepository,
        2: this.communicationEventsRepository,
        3: this.scannerEventRepository,
      };

      const repository = repositoryMapping[type];
      if (!repository) {
        return { message: 'Invalid type' };
      }

      const deleteCriteria = deleteAll ? {} : { id: In(eventIds) };
      const result = await repository.delete(deleteCriteria);

      return { message: `${result.affected} events deleted successfully` };
    } catch (err) {
      return { message: 'Error' };
    }
  }

  async checkDuplicateRow(event: CommunicationEvents) {
    const duplicateEvent = await this.communicationEventsRepository.findOne({
      where: {
        node_id: event.nodeId,
        cooperationClass: event.cooperationClass,
        method: event.method,
        src_node: event.srcNode,
        dest_node: event.destNode,
        message_type: event.messageType,
        created_at: event.createdAt,
      },
    });
    if (duplicateEvent) return true;
    else return false;
  }

  async getEventsList(
    type: number,
    startDate: Date,
    endDate: Date,
    nodeId: string[],
    status: number,
    cooperationClass: string[],
    messageType: string[],
    sessionID: string[],
    communicationClass: string[],
    communicationMethod: string[],
    limit: number,
    page: number,
    sortBy: string,
    sortOrder: string,
  ) {
    let queryBuilder;
    const now = new Date();
    if (type == 1) {
      queryBuilder = this.availabilityEventsRepository
        .createQueryBuilder('events')
        .select([
          'events.id as id',
          'nodes.rsu_id as "nodeId"',
          'nodes.name as "nodeType"',
          'events.cpu_usage as "cpuUsage"',
          'events.cpu_temp as "cpuTemp"',
          'events.ram_usage as "ramUsage"',
          'events.disk_usage as "diskUsage"',
          'events.network_speed as "networkSpeed"',
          'events.network_usage as "networkUsage"',
          'events.network_status as "networkStatus"',
          'events.created_at as "createdAt"',
          'events.detail as "detail"',
        ]);
    } else if (type == 2) {
      queryBuilder = this.communicationEventsRepository
        .createQueryBuilder('events')
        .select([
          'events.id as id',
          'nodes.rsu_id as "nodeId"',
          'nodes.name as "nodeType"',
          'events.src_node as "srcNode"',
          'events.dest_node as "destNode"',
          'events.cooperation_class as "drivingNegotiationsClass"',
          'events.communication_class as "communicationClass"',
          'events.session_id as "sessionID"',
          'events.method as method',
          'events.message_type as "messageType"',
          'events.created_at as "createdAt"',
          'events.detail as "detail"',
        ]);

      queryBuilder = queryBuilder
        .andWhere(
          cooperationClass && cooperationClass.length
            ? 'events.cooperation_class IN (:...cooperationClass)'
            : '1=1',
          { cooperationClass },
        )
        .andWhere(
          messageType && messageType.length
            ? 'events.message_type IN(:...messageType)'
            : '1=1',
          { messageType },
        )
        .andWhere(
          sessionID && sessionID.length
            ? 'events.session_id IN(:...sessionID)'
            : '1=1',
          { sessionID },
        )
        .andWhere(
          communicationClass && communicationClass.length
            ? 'events.communication_class IN(:...communicationClass)'
            : '1=1',
          { communicationClass },
        )
        .andWhere(
          communicationMethod && communicationMethod.length
            ? 'events.method IN(:...communicationMethod)'
            : '1=1',
          { communicationMethod },
        );
    }

    queryBuilder = queryBuilder
      .innerJoin(Nodes, 'nodes', 'events.node_id = nodes.id')
      .andWhere('events.created_at <= :now', { now })
      .andWhere(startDate ? 'events.created_at >= :startDate' : '1=1', {
        startDate,
      })
      .andWhere(endDate ? 'events.created_at < :endDate' : '1=1', {
        endDate,
      })
      .andWhere(
        nodeId && nodeId.length ? 'events.node_id IN (:...nodeIds)' : '1=1',
        { nodeIds: nodeId },
      )
      .andWhere(status ? 'events.status = :status' : '1=1', { status });

    if (sortBy && sortOrder) {
      queryBuilder.orderBy(
        `events.${sortBy}`,
        sortOrder.toUpperCase() as 'ASC' | 'DESC',
      );
    } else {
      queryBuilder.orderBy('events.created_at', 'DESC');
    }

    const events = await queryBuilder
      .limit(limit)
      .offset(page === 1 ? 0 : (page - 1) * limit)
      .getRawMany();

    let mapEvents;
    if (type == 1) {
      mapEvents = events.map((event) => {
        event.networkSpeed = event.networkSpeed ?? '-';
        if (event.networkSpeed != '-') event.networkSpeed += ' Mbps';

        event.networkUsage = event.networkUsage ?? '-';
        if (event.networkUsage != '-') event.networkUsage += ' Byte';

        return {
          id: event.id,
          nodeId: event.nodeId,
          nodeType: event.nodeType,
          detail: event.detail,
          status: event.status,
          createdAt: event.createdAt,
          cpuUsage: event.cpuUsage,
          cpuTemp: event.cpuTemp,
          ramUsage: event.ramUsage,
          diskUsage: event.diskUsage,
          networkSpeed: event.networkSpeed,
          networkUsage: event.networkUsage,
          networkStatus: NetworkStatus[event.networkStatus],
        };
      });
    } else if (type == 2) {
      mapEvents = events.map((event) => {
        return {
          id: event.id,
          nodeId: event.nodeId,
          nodeType: event.nodeType,
          srcNode: event.srcNode,
          destNode: event.destNode,
          cooperationClass: event.drivingNegotiationsClass,
          communicationClass: event.communicationClass,
          sessionID: event.sessionID,
          communicationMethod: event.method,
          messageType: event.messageType,
          detail: event.detail,
          status: event.status,
          createdAt: event.createdAt,
        };
      });
    }

    const total = await queryBuilder.getCount();

    const totalPages = limit ? Math.ceil(total / limit) : 1;

    return {
      events: mapEvents,
      meta: {
        currentPage: page,
        totalPages: totalPages,
        totalItems: total,
        perPage: limit,
      },
    };
  }

  async getEventsSummary(time_range: string) {
    const hourAgo = new Date(new Date().getTime() - 60 * 60 * 1000);

    const availabilityNormal = this.NodesRepo.createQueryBuilder('n')
      .select([
        'n.id as "nodeId"',
        'n.rsu_id as "customId"',
        'COUNT(ae.node_id) as "totalAvailabilityNormal"',
      ])
      .leftJoin(
        AvailabilityEvents,
        'ae',
        'n.id = ae.node_id AND ae.status = 1 AND ae.created_at >= :timestamp',
        {
          timestamp: hourAgo,
        },
      )
      .groupBy('n.id, n.rsu_id');

    const communicationNormal = this.NodesRepo.createQueryBuilder('n')
      .select([
        'n.id as "nodeId"',
        'n.rsu_id as "customId"',
        'COUNT(ce.node_id) as "totalCommunicationNormal"',
      ])
      .leftJoin(
        CommunicationEvents,
        'ce',
        'ce.node_id = n.id AND ce.status = 1 AND ce.created_at >= :timestamp',
        {
          timestamp: hourAgo,
        },
      )
      .groupBy('n.id, n.rsu_id');

    const availabilityError = this.NodesRepo.createQueryBuilder('n')
      .select([
        'n.id as "nodeId"',
        'n.rsu_id as "customId"',
        'COUNT(ae.node_id) as "totalAvailabilityError"',
      ])
      .leftJoin(
        AvailabilityEvents,
        'ae',
        'n.id = ae.node_id AND ae.status = 2 AND ae.created_at >= :timestamp',
        {
          timestamp: hourAgo,
        },
      )
      .groupBy('n.id, n.rsu_id');

    const communicationError = this.NodesRepo.createQueryBuilder('n')
      .select([
        'n.id as "nodeId"',
        'n.rsu_id as "customId"',
        'COUNT(ce.node_id) as "totalCommunicationError"',
      ])
      .leftJoin(
        CommunicationEvents,
        'ce',
        'ce.node_id = n.id AND ce.status = 2 AND ce.created_at >= :timestamp',
        {
          timestamp: hourAgo,
        },
      )
      .groupBy('n.id, n.rsu_id');

    const scannerNormal = this.scannerEventRepository
      .createQueryBuilder('event')
      .select(['COUNT(event.id) as "totalScannerNormal"'])
      .where('event.status = 1 AND event.created_at >= :timestamp', {
        timestamp: hourAgo,
      });

    const scannerError = this.scannerEventRepository
      .createQueryBuilder('event')
      .select(['COUNT(event.id) as "totalScannerError"'])
      .where('event.status = 2 AND event.created_at >= :timestamp', {
        timestamp: hourAgo,
      });

    let summary: any[] = [];

    const [
      availabilityNormalEvents,
      availabilityErrorEvents,
      communicationNormalEvents,
      communicationErrorEvents,
      scannerNormalEvents,
      scannerErrorEvents,
    ] = await Promise.all([
      availabilityNormal.getRawMany(),
      availabilityError.getRawMany(),
      communicationNormal.getRawMany(),
      communicationError.getRawMany(),
      scannerNormal.getRawMany(),
      scannerError.getRawMany(),
    ]);

    const mergedResults = mergeResults(
      availabilityNormalEvents,
      availabilityErrorEvents,
      communicationNormalEvents,
      communicationErrorEvents,
    );

    const totalScannerNormal = +scannerNormalEvents[0].totalScannerNormal;
    const totalScannerError = +scannerErrorEvents[0].totalScannerError;
    const totalScannerEvent = totalScannerNormal + totalScannerError;
    const percentScannerNormal =
      totalScannerEvent > 0
        ? Math.round((totalScannerNormal * 100) / totalScannerEvent)
        : null;
    const percentScannerError =
      totalScannerEvent > 0
        ? Math.round((totalScannerError * 100) / totalScannerEvent)
        : null;
    const percentTotalScanner = totalScannerEvent > 0 ? 100 : null;

    summary = Array.from(mergedResults.values());

    summary.sort((a, b) => a.customId.localeCompare(b.customId));

    const rfScannerSummary = {
      nodeId: 'RF Scanner',
      customId: 'RF Scanner',
      percentCommunicationError: percentScannerError,
      percentCommunicationNormal: percentScannerNormal,
      percentTotalCommunication: percentTotalScanner,
    };

    summary.push(rfScannerSummary);

    return summary;
  }

  async getLatestAvailabilityEvents() {
    const query = `
      WITH latest_event AS (
        SELECT
          e.id,
          e.node_id,
          e.cpu_usage,
          e.cpu_temp,
          e.ram_usage,
          e.disk_usage,
          e.network_speed,
          e.network_usage,
          e.network_status,
          e.created_at,
          ROW_NUMBER() OVER (
            PARTITION BY e.node_id
            ORDER BY e.created_at desc
          ) AS row_number
        FROM public.availability_events AS e
        WHERE e.created_at BETWEEN NOW() - INTERVAL '2 minutes' AND NOW()
      )
    
      SELECT
        n.id AS "nodeID",
        n.rsu_id AS "rsuID",
        n.status AS "nodeStatus",
        n.name AS "rsuName",
        le.cpu_usage AS "cpuUsage",
        le.cpu_temp AS "cpuTemp",
        le.ram_usage AS "ramUsage",
        le.disk_usage AS "diskUsage",
        le.network_speed AS "networkSpeed",
        le.network_usage AS "networkUsage",
        le.network_status AS "networkStatus",
        le.created_at AS "createdAt"
      FROM public.nodes AS n
      LEFT JOIN latest_event as le ON n.id = le.node_id
      WHERE le.row_number = 1 OR le.row_number IS NULL 
      ORDER BY rsu_id ASC
    `;

    const queryResult = await this.availabilityEventsRepository.query(query);

    const result = queryResult.map((e) => {
      return {
        nodeID: e.nodeID,
        rsuID: e.rsuID,
        rsuName: e.rsuName,
        nodeStatus: e.nodeStatus,
        cpuUsage: e.cpuUsage,
        cpuTemp: e.cpuTemp,
        ramUsage: e.ramUsage,
        diskUsage: e.diskUsage,
        networkSpeed: e.networkSpeed,
        networkUsage: e.networkUsage,
        networkStatus:
          e.networkStatus != null ? NetworkStatus[e.networkStatus] : null,
        createdAt: e.createdAt,
      };
    });
    return result;
  }

  async getSystemStatus() {
    const nodeStatus = await this.getLatestAvailabilityEvents();
    const additionalNode = [
      {
        nodeID: 'RF Scanner',
        rsuID: 'RF Scanner',
        rsuName: 'RF Scanner',
        nodeStatus: 1,
        cpuUsage: null,
        cpuTemp: null,
        ramUsage: null,
        diskUsage: null,
        networkSpeed: null,
        networkUsage: null,
        networkStatus: null,
        createdAt: null,
      },
    ];

    additionalNode.forEach((item) => {
      nodeStatus.push(item);
    });
    return nodeStatus;
  }

  async exportLogData(type: number, eventIds: string[], log: boolean) {
    try {
      if (type == 1) {
        const result = await this.availabilityEventsRepository
          .createQueryBuilder('events')
          .select([
            'nodes.rsu_id as "nodeId"',
            'nodes.name as "nodeType"',
            'events.cpu_usage as "cpuUsage"',
            'events.cpu_temp as "cpuTemp"',
            'events.ram_usage as "ramUsage"',
            'events.disk_usage as "diskUsage"',
            'events.network_speed as "networkSpeed"',
            'events.network_usage as "networkUsage"',
            'events.network_status as "networkStatus"',
            'events.created_at as "createdAt"',
            'events.detail as detail',
          ])
          .innerJoin(Nodes, 'nodes', 'events.node_id = nodes.id')
          .where({ id: In(eventIds) })
          .getRawMany();

        return log
          ? result.map((event) => {
              return {
                [Event_Key.OCCURRENCE_TIME]: convertTimeZone(event.createdAt),
                [Event_Key.NODE_ID]: event.nodeId,
                [Event_Key.NODE_TYPE]: event.nodeType,
                [Event_Key.CPU_USAGE]: event.cpuUsage,
                [Event_Key.CPU_TEMPERATURE]: event.cpuTemp,
                [Event_Key.RAM_USAGE]: event.ramUsage,
                [Event_Key.DISK_USAGE]: event.diskUsage,
                [Event_Key.NETWORK_SPEED]: event.networkSpeed
                  ? event.networkSpeed + ' Mbps'
                  : null,
                [Event_Key.NETWORK_USAGE]: event.networkUsage
                  ? event.networkUsage + ' Byte'
                  : null,
                [Event_Key.NETWORK_CONNECTION_STATUS]:
                  NetworkStatus[event.networkStatus],
                [Event_Key.DETAIL]: event.detail,
              };
            })
          : result.map((event) => {
              return {
                [Event_Key.OCCURRENCE_TIME]: convertTimeZone(event.createdAt),
                [Event_Key.NODE_ID]: event.nodeId,
                [Event_Key.CPU_USAGE]: event.cpuUsage,
                [Event_Key.CPU_TEMPERATURE]: event.cpuTemp,
                [Event_Key.RAM_USAGE]: event.ramUsage,
                [Event_Key.DISK_USAGE]: event.diskUsage,
                [Event_Key.NETWORK_SPEED]: event.networkSpeed
                  ? event.networkSpeed + ' Mbps'
                  : null,
                [Event_Key.NETWORK_USAGE]: event.networkUsage
                  ? event.networkUsage + ' Byte'
                  : null,
                [Event_Key.NETWORK_CONNECTION_STATUS]:
                  NetworkStatus[event.networkStatus],
              };
            });
      } else if (type == 2) {
        const result = await this.communicationEventsRepository
          .createQueryBuilder('events')
          .select([
            'events.id as id',
            'nodes.rsu_id as "nodeId"',
            'nodes.name as "nodeType"',
            'events.src_node as "srcNode"',
            'events.dest_node as "destNode"',
            'events.cooperation_class as "cooperationClass"',
            'events.session_id as "sessionID"',
            'events.communication_class as "communicationClass"',
            'events.method as method',
            'events.message_type as "messageType"',
            'events.status as status',
            'events.created_at as "createdAt"',
            'events.detail as detail',
          ])
          .innerJoin(Nodes, 'nodes', 'events.node_id = nodes.id')
          .where({ id: In(eventIds) })
          .getRawMany();

        return log
          ? result.map((event) => {
              return {
                [Event_Key.OCCURRENCE_TIME]: convertTimeZone(event.createdAt),
                [Event_Key.NODE_ID]: event.nodeId,
                [Event_Key.NODE_TYPE]: event.nodeType,
                [Event_Key.SRC_NODE]:
                  event.srcNode != null ? event.srcNode : '-',
                [Event_Key.DEST_NODE]:
                  event.destNode != null ? event.destNode : '-',
                [Event_Key.COOPERATION_CLASS]: event.cooperationClass,
                [Event_Key.SESSION_ID]: event.sessionID,
                [Event_Key.COMMUNICATION_CLASS]: event.communicationClass,
                [Event_Key.METHOD]: event.method,
                [Event_Key.MESSAGE_TYPE]: event.messageType,
                [Event_Key.DETAIL]: event.detail,
              };
            })
          : result.map((event) => {
              return {
                [Event_Key.OCCURRENCE_TIME]: convertTimeZone(event.createdAt),
                [Event_Key.NODE_ID]: event.nodeId,
                [Event_Key.SRC_NODE]:
                  event.srcNode != null ? event.srcNode : '-',
                [Event_Key.DEST_NODE]:
                  event.destNode != null ? event.destNode : '-',
                [Event_Key.COOPERATION_CLASS]: event.cooperationClass,
                [Event_Key.SESSION_ID]: event.sessionID,
                [Event_Key.COMMUNICATION_CLASS]: event.communicationClass,
                [Event_Key.METHOD]: event.method,
                [Event_Key.MESSAGE_TYPE]: event.messageType,
              };
            });
      } else if (type === 3) {
        const result = await this.scannerEventRepository
          .createQueryBuilder('event')
          .select([
            'event.signal_num as "signalNum"',
            'event.signal_id as "signalId"',
            'event.set_num as "setNum"',
            'event.center_freq as "centerFreq"',
            'event.bandwidth as "bandwidth"',
            'event.elevation as "elevation"',
            'event.azimuth as "azimuth"',
            'event.signal_power as "signalPower"',
            'event.signal_class as "signalClass"',
            'event.created_at as "createdAt"',
          ])
          .where({ id: In(eventIds) })
          .getRawMany();

        return log
          ? result.map((event) => {
              return {
                [Event_Key.OCCURRENCE_TIME]: convertTimeZone(event.createdAt),
                [Event_Key.SIGNAL_NUM]: event.signalNum,
                [Event_Key.SIGNAL_ID]: event.signalId,
                [Event_Key.SET_NUM]: event.setNum,
                [Event_Key.CENTER_FREQ]: event.centerFreq,
                [Event_Key.BANDWIDTH]: event.bandwidth,
                [Event_Key.ELEVATION]: event.elevation,
                [Event_Key.AZIMUTH]: event.azimuth ? event.azimuth : null,
                [Event_Key.SIGNAL_POWER]: event.signalPower
                  ? event.signalPower
                  : null,
                [Event_Key.SIGNAL_CLASS]: event.signalClass,
              };
            })
          : result.map((event) => {
              return {
                [Event_Key.OCCURRENCE_TIME]: convertTimeZone(event.createdAt),
                [Event_Key.SIGNAL_NUM]: event.signalNum,
                [Event_Key.SIGNAL_ID]: event.signalId,
                [Event_Key.SET_NUM]: event.setNum,
                [Event_Key.CENTER_FREQ]: event.centerFreq,
                [Event_Key.BANDWIDTH]: event.bandwidth,
                [Event_Key.ELEVATION]: event.elevation,
                [Event_Key.AZIMUTH]: event.azimuth ? event.azimuth : null,
                [Event_Key.SIGNAL_POWER]: event.signalPower
                  ? event.signalPower
                  : null,
                [Event_Key.SIGNAL_CLASS]: SignalClass[event.signalClass],
              };
            });
      }
    } catch (error) {
      console.error('Error exporting data:', error);
      throw error;
    }
  }

  async saveBatch(
    records: any,
    typeEvent: number,
    nodeMap: any,
    offset: number,
    errorRecords: any,
  ) {
    const { validEvents, inValidEvents } = await this.validateEvent(
      records,
      typeEvent,
      {
        nodeInfo: nodeMap,
        offset,
      },
    );

    errorRecords.push(...inValidEvents);

    await this.saveEvent(typeEvent, validEvents);

    offset += records.length;
    return {
      errorRecords: errorRecords,
      offset: offset,
    };
  }

  async defineErrorMessage(type: number, event: any) {
    const details = [];
    if (type == 1) {
      if (+event.cpuTemp > 70 || event.cpu_temp > 70) {
        details.push('높은 CPU 온도');
      }

      if (+event.cpuUsage > 70 || event.cpu_usage > 70) {
        details.push('높은 CPU 사용량');
      }

      if (+event.ramUsage > 80 || event.ram_usage > 80) {
        details.push('높은 RAM 사용량');
      }

      if (+event.diskUsage > 80 || event.disk_usage > 80) {
        details.push('높은 DISK 사용량');
      }

      if (+event.networkStatus == 2 || event.network_status == 2) {
        details.push('네트워크 오류');
      }
    } else if (type == 2) {
      let isDuplicated = await this.checkDuplicateRow(event);
      if (isDuplicated == true) {
        details.push('중복 메시지 수신');
      }
    } else if (type == 3) {
      if (+event.signalClass == 1) {
        details.push('재밍신호');
      }
    }

    let detailMessage = '';
    if (details.length != 0) {
      if (details.length === 1) {
        detailMessage += `${details[0]}`;
      } else if (details.length === 2) {
        detailMessage += `${details[0]} & ${details[1]}`;
      } else {
        const lastDetail = details.pop();
        detailMessage += `${details.join(', ')} & ${lastDetail}`;
      }
    }
    return detailMessage;
  }

  async parseCsvAndSaveToDatabase(filePath: string) {
    try {
      const records: any[] = [];

      await new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
          .pipe(fastcsv.parse({ headers: true }))
          .on('data', (row) => {
            records.push(row);
          })
          .on('end', () => {
            resolve(records);
          })
          .on('error', (error) => {
            reject(error);
          });
      });

      let batchRecords = [];
      const batchSize = 5000;
      let offset = 2;
      let errorRecords = [];
      let typeEvent = isValidHeader(records[0]);
      if (typeEvent == 0) throw new Error('Wrong file format');
      const nodeMap = (await this.nodeService.getMapNodeList()).customMap;

      for (const record of records) {
        batchRecords.push(record);
        if (batchRecords.length >= batchSize) {
          const batchResult = await this.saveBatch(
            batchRecords,
            typeEvent,
            nodeMap,
            offset,
            errorRecords,
          );
          errorRecords = batchResult.errorRecords;
          offset = batchResult.offset;
          batchRecords = [];
        }
      }

      if (batchRecords.length > 0) {
        const batchResult = await this.saveBatch(
          batchRecords,
          typeEvent,
          nodeMap,
          offset,
          errorRecords,
        );
        errorRecords = batchResult.errorRecords;
        offset = batchResult.offset;
      }

      if (errorRecords.length == records.length) throw new Error();
      let message = '';
      if (errorRecords.length != 0)
        message = ` file ${filePath.replace(
          /.*[\\/]/,
          '',
        )} at line ${errorRecords} `;
      return {
        status: 'success',
        message: message,
        errorRecords: errorRecords,
      };
    } catch (error) {
      return {
        status: 'error',
        message: 'Failed to import data! Wrong file format',
      };
    }
  }

  async mapInformationEvent(
    record: any,
    typeEvent: number,
    additionalInfo: any,
  ) {
    try {
      if (typeEvent == 1) {
        const node = additionalInfo.nodeInfo.get(record[Event_Key.NODE_ID]);
        let cpuUsage = parseFloat(record[Event_Key.CPU_USAGE]);
        cpuUsage = cpuUsage < 0 || cpuUsage > 100 ? null : cpuUsage;

        let cpuTemp = parseFloat(record[Event_Key.CPU_TEMPERATURE]);
        cpuTemp = cpuTemp < 0 || cpuTemp > 100 ? null : cpuTemp;

        let ramUsage = parseFloat(record[Event_Key.RAM_USAGE]);
        ramUsage = ramUsage < 0 || ramUsage > 100 ? null : ramUsage;

        let diskUsage = parseFloat(record[Event_Key.DISK_USAGE]);
        diskUsage = diskUsage < 0 || diskUsage > 100 ? null : diskUsage;

        let networkStatus = getEnumValue(
          NetworkStatus,
          record[Event_Key.NETWORK_CONNECTION_STATUS],
        );

        let networkSpeed = parseFloat(record[Event_Key.NETWORK_SPEED]);
        networkSpeed =
          networkSpeed < 0 || networkSpeed > 100 ? null : networkSpeed;

        let networkUsage = parseFloat(record[Event_Key.NETWORK_USAGE]);
        networkUsage = networkUsage < 0 ? null : networkUsage;

        if (
          networkStatus == 2 &&
          (record[Event_Key.NETWORK_USAGE] != '' ||
            record[Event_Key.NETWORK_SPEED] != '')
        ) {
          throw new Error();
        }

        const event = new AvailabilityEvents();
        event.nodeId =
          node ??
          (() => {
            throw new Error();
          })();
        event.cpuUsage =
          cpuUsage ??
          (() => {
            throw new Error();
          })();
        event.cpuTemp =
          cpuTemp ??
          (() => {
            throw new Error();
          })();
        event.ramUsage =
          ramUsage ??
          (() => {
            throw new Error();
          })();
        event.diskUsage =
          diskUsage ??
          (() => {
            throw new Error();
          })();
        event.networkStatus =
          networkStatus ??
          (() => {
            throw new Error();
          })();
        event.networkSpeed =
          typeof networkSpeed !== 'undefined'
            ? Number.isNaN(networkSpeed)
              ? null
              : networkSpeed
            : (() => {
                throw new Error();
              })();
        event.networkUsage =
          typeof networkUsage !== 'undefined'
            ? Number.isNaN(networkUsage)
              ? null
              : networkUsage
            : (() => {
                throw new Error();
              })();
        event.createdAt = moment(
          convertToUTC(record[Event_Key.OCCURRENCE_TIME]),
        ).toDate();
        event.detail = await this.defineErrorMessage(1, event);
        event.status = event.detail != '' ? 2 : 1;

        return {
          status: 1,
          event: event,
        };
      } else if (typeEvent == 2) {
        const event = new CommunicationEvents();

        event.nodeId = additionalInfo.nodeInfo.get(record[Event_Key.NODE_ID]);
        event.destNode = record[Event_Key.DEST_NODE];
        event.srcNode = record[Event_Key.SRC_NODE];

        event.cooperationClass =
          record[Event_Key.COOPERATION_CLASS] ??
          (() => {
            throw new Error();
          })();
        event.sessionId =
          record[Event_Key.SESSION_ID] ??
          (() => {
            throw new Error();
          })();
        event.communicationClass =
          record[Event_Key.COMMUNICATION_CLASS] ??
          (() => {
            throw new Error();
          })();
        event.method =
          record[Event_Key.METHOD] ??
          (() => {
            throw new Error();
          })();
        event.messageType =
          record[Event_Key.MESSAGE_TYPE] ??
          (() => {
            throw new Error();
          })();
        event.createdAt = moment(
          convertToUTC(record[Event_Key.OCCURRENCE_TIME]),
        ).toDate();
        event.detail = '';
        event.status = event.detail != '' ? 2 : 1;

        return {
          status: 1,
          event: event,
        };
      } else if (typeEvent == 3) {
        let signalId = parseInt(record[Event_Key.SIGNAL_ID]);
        signalId = signalId < 0 ? null : signalId;

        let signalNum = parseInt(record[Event_Key.SIGNAL_NUM]);
        signalNum = signalNum < 0 ? null : signalNum;

        let setNum = parseInt(record[Event_Key.SET_NUM]);
        setNum = setNum < 0 ? null : setNum;

        let centerFreq = parseFloat(record[Event_Key.CENTER_FREQ]);
        centerFreq = centerFreq < 0 ? null : centerFreq;

        let bandwidth = parseFloat(record[Event_Key.BANDWIDTH]);
        bandwidth = bandwidth < 0 ? null : bandwidth;

        let elevation = parseFloat(record[Event_Key.ELEVATION]);
        elevation = elevation < 0 ? null : elevation;

        let azimuth = parseFloat(record[Event_Key.AZIMUTH]);

        let signalPower = parseFloat(record[Event_Key.SIGNAL_POWER]);
        signalPower = signalPower < 0 ? null : signalPower;

        let signalClass = parseInt(record[Event_Key.SIGNAL_CLASS]);
        signalClass = signalClass < 0 || signalClass > 1 ? null : signalClass;

        const event = new ScannerEvents();
        event.signalId =
          signalId ??
          (() => {
            throw new Error();
          })();
        event.signalNum =
          signalNum ??
          (() => {
            throw new Error();
          })();
        event.setNum =
          setNum ??
          (() => {
            throw new Error();
          })();
        event.centerFreq =
          centerFreq ??
          (() => {
            throw new Error();
          })();
        event.bandwidth =
          bandwidth ??
          (() => {
            throw new Error();
          })();
        event.elevation =
          elevation ??
          (() => {
            throw new Error();
          })();
        event.azimuth =
          azimuth ??
          (() => {
            throw new Error();
          })();
        event.signalPower =
          signalPower ??
          (() => {
            throw new Error();
          })();
        event.signalClass =
          signalClass ??
          (() => {
            throw new Error();
          })();
        event.createdAt = moment(
          convertToUTC(record[Event_Key.OCCURRENCE_TIME]),
        ).toDate();
        event.detail = await this.defineErrorMessage(3, event);
        event.status = event.detail != '' ? 2 : 1;

        return {
          status: 1,
          event: event,
        };
      } else {
        throw new Error();
      }
    } catch (err) {
      console.error('dddddddd', record);
      return {
        status: 2,
        event: record,
      };
    }
  }

  async validateEvent(events: any, typeEvent: number, additionalInfo: any) {
    const validEvents = [];
    const invalidEventIndexes = [];
    const offset = additionalInfo.offset ?? 0;

    for (let i = 0; i < events.length; i++) {
      const result = await this.mapInformationEvent(
        events[i],
        typeEvent,
        additionalInfo,
      );
      if (result.status == 1) {
        validEvents.push(result.event);
      } else {
        invalidEventIndexes.push(offset + i);
      }
    }
    return { validEvents: validEvents, inValidEvents: invalidEventIndexes };
  }

  async pushNotification(username: any) {
    const lastUpdated = new Date();
    const now = new Date();
    lastUpdated.setMinutes(lastUpdated.getMinutes() - 30);
    const listIgnoreEvents =
      await this.ignoreEventsService.getIgnoreEventByUsername(username);

    let avaiEventsQuery = this.availabilityEventsRepository
      .createQueryBuilder('events')
      .select([
        'events.id as id',
        'nodes.rsu_id as "nodeId"',
        'events.detail as detail',
        'events.created_at as "createAt"',
      ])
      .innerJoin(Nodes, 'nodes', 'events.node_id = nodes.id')
      .where('events.created_at > :lastUpdated AND events.created_at <= :now', {
        lastUpdated,
        now,
      })
      .andWhere('events.status = 2')
      .orderBy('events.created_at', 'DESC');

    if (listIgnoreEvents && listIgnoreEvents.length > 0) {
      avaiEventsQuery = avaiEventsQuery.andWhere(
        'events.id NOT IN (:...listIgnoreEvents)',
        { listIgnoreEvents },
      );
    }

    const avaiEvents = await avaiEventsQuery.getRawMany();

    let commEventsQuery = this.communicationEventsRepository
      .createQueryBuilder('events')
      .select([
        'events.id as id',
        'nodes.rsu_id as "nodeId"',
        'events.detail as detail',
        'events.created_at as "createAt"',
      ])
      .innerJoin(Nodes, 'nodes', 'events.node_id = nodes.id')
      .where('events.created_at > :lastUpdated AND events.created_at <= :now', {
        lastUpdated,
        now,
      })
      .andWhere('events.status = 2')
      .orderBy('events.created_at', 'DESC');

    if (listIgnoreEvents && listIgnoreEvents.length > 0) {
      commEventsQuery = commEventsQuery.andWhere(
        'events.id NOT IN (:...listIgnoreEvents)',
        { listIgnoreEvents },
      );
    }

    const commEvents = await commEventsQuery.getRawMany();

    const queryResults = [...avaiEvents, ...commEvents];
    const cleanResult = queryResults.map((event) => {
      return {
        id: event.id,
        nodeId: event.nodeId,
        detail: event.detail,
        status: event.status,
        createAt: event.createAt,
      };
    });
    return cleanResult;
  }

  async getRSUUsage(type: string, period: string): Promise<any> {
    const listNodeLive = (await this.nodeService.findAll()).nodes;

    let result = [];
    await Promise.all(
      (
        await listNodeLive
      ).map(async (rsu) => {
        try {
          const fromDate = new Date();
          const now = new Date();

          let rsuUsage;

          // Last 30 days
          if (period === 'month') {
            fromDate.setDate(fromDate.getDate() - 30);
            now.setDate(now.getDate() + 1);

            fromDate.setHours(0, 0, 0, 0);
            now.setHours(0, 0, 0, 0);

            rsuUsage = (await this.availabilityEventsRepository
              .createQueryBuilder('event')
              .select(
                `DATE(event.created_at) AS timestamp, AVG(event.${type}_usage) AS average`,
              )
              .where(
                'event.node_id = :rsuId AND event.created_at >= :fromDate',
                { rsuId: rsu.id, fromDate },
              )
              .groupBy('DATE(event.created_at)')
              .orderBy('timestamp')
              .execute()) as Promise<{ date: string; average: number }[]>;

            rsuUsage = spreadDataWithTimeStamp(rsuUsage, fromDate, now, period);
          }
          // Last 24 hours
          if (period === 'date') {
            fromDate.setHours(fromDate.getHours() - 23);
            now.setHours(now.getHours());

            rsuUsage = (await this.availabilityEventsRepository
              .createQueryBuilder('usage')
              .select(
                `DATE_TRUNC('hour', usage.created_at) AS timestamp, AVG(usage.${type}_usage) AS average`,
              )
              .where(
                'usage.node_id = :rsuId AND usage.created_at >= :fromDate',
                { rsuId: rsu.id, fromDate },
              )
              .groupBy("DATE_TRUNC('hour', usage.created_at)")
              .orderBy('timestamp')
              .execute()) as Promise<{ hour: Date; average: number }[]>;

            rsuUsage = spreadDataWithTimeStamp(rsuUsage, fromDate, now, period);
          }
          // Last 60 minutes
          if (period === 'hour') {
            fromDate.setMinutes(fromDate.getMinutes() - 60);
            now.setMinutes(now.getMinutes() + 2);

            rsuUsage = (await this.availabilityEventsRepository
              .createQueryBuilder('usage')
              .select(
                `DATE_TRUNC('minute', usage.created_at) AS timestamp, AVG(usage.${type}_usage) AS average`,
              )
              .where(
                'usage.node_id = :rsuId AND usage.created_at >= :fromDate',
                { rsuId: rsu.id, fromDate },
              )
              .groupBy("DATE_TRUNC('minute', usage.created_at)")
              .orderBy('timestamp')
              .execute()) as Promise<{ minute: string; average: number }[]>;

            rsuUsage = spreadDataWithTimeStamp(rsuUsage, fromDate, now, period);
          }

          rsuUsage !== undefined &&
            rsuUsage.length !== 0 &&
            result.push({
              id: rsu.rsuID,
              usage: rsuUsage,
            });
        } catch (error) {
          console.error(error);
        }
      }),
    );
    return result.sort((a, b) => a.id.localeCompare(b.id));
  }

  async parseDataToAvailEvent(message: any) {
    const node = await this.nodeService.findOne({ rsuID: message.nodeID });

    const event = new AvailabilityEvents();

    event.nodeId = node.id;
    event.cpuUsage = message.cpuUsage;
    event.cpuTemp = message.cpuTemperature;
    event.ramUsage = message.ramUsage;
    event.diskUsage = message.diskUsage;
    event.networkStatus = message.rsuConnection == true ? 1 : 2;
    event.networkSpeed =
      message.networkSpeed != null ? message.networkSpeed : null;
    event.networkUsage =
      message.networkUsage != null ? message.networkUsage : null;

    if (message.timeStamp) {
      event.createdAt = convertUnixToFormat(message.timeStamp);
    }

    event.detail = await this.defineErrorMessage(1, event);

    event.status = event.detail.length > 0 ? 2 : 1;

    // save live status
    await this.saveAliveStatus(event.nodeId, message.timeStamp);

    return event;
  }

  async parseDataToCommEvent(data: any): Promise<CommunicationEvents[]> {
    const messageList = data.messageList;
    const nodeIDMap = (await this.nodeService.getMapNodeList()).customMap;
    const eventList: CommunicationEvents[] = [];

    for (let message of messageList) {
      try {
        const nodeID = nodeIDMap.get(message.nodeID);
        const srcNodeID = message.senderNodeID;
        const destNodeID = message.receiverNodeID;

        if (!nodeID) {
          throw new Error(`NodeID not found for RSU: ${message.nodeID}`);
        }

        const event = new CommunicationEvents();
        event.nodeId = nodeID;
        event.cooperationClass = message.cooperationClass;
        event.sessionId = message.sessionID;
        event.messageType = message.messageType;
        event.method = message.communicationType;
        event.communicationClass = message.communicationClass;
        event.destNode = destNodeID;
        event.srcNode = srcNodeID;
        event.detail = '';
        event.status = event.detail.length > 0 ? 2 : 1;
        event.createdAt = convertUnixToFormat(message.timeStamp);

        // update alive status
        await this.saveAliveStatus(event.nodeId, message.timeStamp);

        eventList.push(event);
      } catch (error) {
        // Handle or log the error for the specific event, and continue with the loop
        console.error(
          `Error processing event: ${JSON.stringify(message)}`,
          error,
        );
      }
    }

    return eventList;
  }

  async parseDataToScannerEvent(data: IScannerEvent): Promise<ScannerEvents> {
    const event = new ScannerEvents();

    event.azimuth = data.azimuth;
    event.bandwidth = data.bandwidth;
    event.centerFreq = data.centerFreq;
    event.elevation = data.elevation;
    event.setNum = data.setNum;
    event.signalClass = data.signalClass;
    event.signalId = data.signalId;
    event.signalNum = data.signalNum;
    event.signalPower = data.signalPower;
    event.createdAt = data.timeStamp;

    event.detail = await this.defineErrorMessage(3, event);

    event.status = event.detail.length > 0 ? 2 : 1;

    return event;
  }

  async saveAliveStatus(nodeID: string, timestamp: number) {
    await this.NodesRepo.createQueryBuilder()
      .update(Nodes)
      .set({
        status: 0,
        lastAliveAt: convertUnixToFormat(timestamp),
      })
      .where('id = :id', { id: nodeID })
      .execute();
  }

  async parseDataToKeepAlive(message: any) {
    const node = await this.nodeService.findOne({ rsuID: message.nodeID });
    const result = await this.saveAliveStatus(node.id, message.timeStamp);
  }

  // async parseDataToDoorStatus(message: any) {
  //   const doorStatus = new DoorStatus();
  //   doorStatus.status = message.doorStatus;
  //   doorStatus.createdAt = convertUnixToFormat(message.timestamp);
  //   await this.doorStatusRepository.save(doorStatus);
  // }

  async getScannerEventList(
    startDate: Date,
    endDate: Date,
    sortBy: string,
    sortOrder: string,
    status: number,
    page: number,
    limit: number,
  ) {
    const queryBuilder = this.scannerEventRepository
      .createQueryBuilder('scanner_events')
      .select([
        'scanner_events.id AS "id"',
        'scanner_events.signal_num AS "signalNum"',
        'scanner_events.signal_id AS "signalId"',
        'scanner_events.set_num AS "setNum"',
        'scanner_events.center_freq AS "centerFreq"',
        'scanner_events.bandwidth AS "bandwidth"',
        'scanner_events.elevation AS "elevation"',
        'scanner_events.azimuth AS "azimuth"',
        'scanner_events.signal_power AS "signalPower"',
        'scanner_events.signal_class AS "signalClass"',
        'scanner_events.created_at AS timestamp',
      ])
      .andWhere(startDate ? 'scanner_events.created_at >= :startDate' : '1=1', {
        startDate,
      })
      .andWhere(endDate ? 'scanner_events.created_at < :endDate' : '1=1', {
        endDate,
      })
      .andWhere(status ? 'scanner_events.status = :status' : '1=1', { status });

    if (sortBy && sortOrder) {
      queryBuilder.orderBy(
        `scanner_events.${sortBy}`,
        sortOrder.toUpperCase() as 'ASC' | 'DESC',
      );
    } else {
      queryBuilder.orderBy('scanner_events.created_at', 'DESC');
    }

    const result = await queryBuilder
      .limit(limit)
      .offset(page == 1 ? 0 : (page - 1) * limit)
      .getRawMany();

    const total = await queryBuilder.getCount();

    const totalPages = limit ? Math.ceil(total / limit) : 1;

    return {
      events: result,
      meta: {
        currentPage: page,
        totalPages: totalPages,
        totalItems: total,
        perPage: limit,
      },
    };
  }

  // async cronJobUpdateAvailData() {
  //   // get latest data
  //   const res = await HttpHelper.post({
  //     url: `${process.env.EDGE_SYSTEM_DOMAIN}/edge/status`,
  //     headers: {
  //       'api-key': process.env.API_KEY,
  //     },
  //   });

  //   console.log(res);
  //   if (res?.status !== 200) {
  //     return;
  //   }
  // }
  //   const eventList = [];
  //   const statusList = res.data.statusList;

  //   for (let status of statusList) {
  //     let node = await this.nodeService.findOne({ customId: status.nodeID });

  //     if (!node) {
  //       node = new Nodes();
  //       node.customId = status.nodeID;
  //     }
  //     node.name = status.rsuName;
  //     node.latitude = status.latitude;
  //     node.longitude = status.longitude;

  //     const result = await this.NodesRepo.save(node);

  //     let event = new AvailabilityEvents();
  //     event.nodeId = result.id;
  //     event.createdAt = convertUnixToFormat(status.timeStamp);
  //     event.cpuUsage = status.cpuUsage;
  //     event.cpuTemp = status.cpuTemperature;
  //     event.ramUsage = status.ramUsage;
  //     event.diskUsage = status.diskUsage;
  //     event.networkStatus = status.rsuConnection == true ? 1 : 2;
  //     event.networkSpeed = status.networkSpeed ? status.networkSpeed : null;
  //     event.networkUsage = status.networkUsage ? status.networkUsage : null;
  //     event.detail = await this.defineErrorMessage(1, event);
  //     event.status = event.detail.length > 0 ? 2 : 1;

  //     eventList.push(event);
  //   }

  //   await this.availabilityEventsRepository.save(eventList);
  // }

  // @Cron('0 */1 * * * *')
  // async cronJobUpdateCommData() {
  //   // get latest data
  //   const res = await HttpHelper.get({
  //     url: `${process.env.EDGE_SYSTEM_DOMAIN}/edge/message`,
  //     headers: {
  //       'api-key': process.env.X_API_KEY,
  //     },
  //   });

  //   if (res?.status !== 200) {
  //     return;
  //   }

  //   const messageList = res.data.messageList;

  //   const nodeIDMap = (await this.nodeService.getMapNodeList()).customMap;
  //   const eventList = [];

  //   for (let message of messageList) {
  //     const event = new CommunicationEvents();
  //     event.createdAt = convertUnixToFormat(message.timeStamp);
  //     event.nodeId = nodeIDMap.get(message.nodeID);
  //     event.cooperationClass = message.cooperationClass;
  //     event.sessionId = message.sessionID;
  //     event.messageType = message.messageType;
  //     event.method = message.communicationType;
  //     event.communicationClass = message.communicationClass;

  //     event.destNode =
  //       message.receiverNodeID != 'B'
  //         ? nodeIDMap.get(message.receiverNodeID)
  //         : null;

  //     event.srcNode =
  //       message.senderNodeID != 'B'
  //         ? nodeIDMap.get(message.senderNodeID)
  //         : null;

  //     eventList.push(event);
  //   }
  //   await this.communicationEventsRepository.save(eventList);
  // }

  async testTime() {
    const now = new Date();
    const fromDate = new Date();

    fromDate.setDate(fromDate.getDate() - 29);
    now.setDate(now.getDate() + 1);

    fromDate.setHours(0, 0, 0, 0);
    now.setHours(0, 0, 0, 0);

    return generateTimestamps(fromDate, now, 'month');
  }
}

const generateTimestamps = (
  fromDate: Date,
  now: Date,
  type: string,
): Date[] => {
  const timestamps: Date[] = [];
  let currentTimestamp = new Date(fromDate);

  while (currentTimestamp <= now) {
    if (type == 'hour') {
      currentTimestamp.setMinutes(currentTimestamp.getMinutes() + 0, 0, 0);
    } else if (type == 'date') {
      currentTimestamp.setHours(currentTimestamp.getHours() + 0, 0, 0, 0);
    } else {
      currentTimestamp.setDate(currentTimestamp.getDate() + 0);
    }

    const timestamp = new Date(currentTimestamp);
    timestamps.push(timestamp);

    if (type == 'hour') {
      currentTimestamp.setMinutes(currentTimestamp.getMinutes() + 1, 0, 0);
    } else if (type == 'date') {
      currentTimestamp.setHours(currentTimestamp.getHours() + 1, 0, 0);
    } else {
      currentTimestamp.setDate(currentTimestamp.getDate() + 1);
    }
  }

  return timestamps;
};

const spreadDataWithTimeStamp = (
  rsuUsage: any,
  startDate: Date,
  endDate: Date,
  type: string,
) => {
  const timestamp = generateTimestamps(startDate, endDate, type);
  const resultMap = new Map(
    rsuUsage.map((entry) => [String(entry.timestamp), entry]),
  );

  rsuUsage = timestamp.map((ts) => {
    const resultEntry = resultMap.get(String(ts));
    return resultEntry ? resultEntry : { timestamp: ts, average: null };
  });

  return rsuUsage;
};
