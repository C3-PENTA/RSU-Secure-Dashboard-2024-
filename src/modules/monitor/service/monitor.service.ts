import { Inject, Injectable, forwardRef } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { CommunicationEvents } from 'src/modules/events/entity/communication-events.entity';
import { EventsService } from 'src/modules/events/service/events.service';
import { NodeService } from 'src/modules/nodes/service/nodes.service';
import { Repository } from 'typeorm';
import { enumToKeyValue } from '@util/handleEnumValue';
import { EventStatus, NetworkStatus } from 'src/constants';
import {
  IAvailEvent,
  ICommEventList,
  IKeepAliveMessage,
  IScannerEvent,
} from '@interface/event.interface';
import { GatewayService } from 'src/modules/gateway/service/gateway.service';
import axios from 'axios'
import { ScannerEventDTO } from '../dto/event-prop.dto';
import { formatTimestamp } from '@util/function';

interface RangeInf {
  min: number;
  max: number;
}

interface NodeProp {
  nodeID: string;
  rsuName: string;
  latitude?: number;
  longitude?: number;
}

interface AvailEventPropGenInf {
  nodes: NodeProp[];
  cpuUsage: RangeInf;
  cpuTemperature: RangeInf;
  ramUsage: RangeInf;
  diskUsage: RangeInf;
  networkSpeed: RangeInf;
  networkUsage: RangeInf;
}

interface CommEventPropGenInf {
  nodes: NodeProp[];
  cooperationClass: string[];
  sessionID: RangeInf;
  messageType: string[];
}

@Injectable()
export class MonitorService {
  private autoRefresh: boolean;
  public isCronJobEnabled: boolean;

  // private AvailEventProp: AvailEventPropGenInf;
  // private CommEventProp: CommEventPropGenInf;

  constructor(
    @InjectRepository(CommunicationEvents)
    private commEventsRepo: Repository<CommunicationEvents>,
    private nodeService: NodeService,
    private eventService: EventsService,
    private gatewayService: GatewayService,
  ) {
    this.autoRefresh = true;
    this.isCronJobEnabled = false;
  }

  setAutoRefresh(state: boolean): void {
    this.autoRefresh = state;
  }

  getAutoRefresh(): boolean {
    return this.autoRefresh;
  }


  async getMetadata() {
    const nodeList = await this.nodeService.findAll();
    const nodeMap = {};
    for (const node of nodeList.nodes) {
      nodeMap[node.rsuID] = node.id;
    }

    const cooperationClass = await this.commEventsRepo
      .createQueryBuilder('comm_event')
      .select('comm_event.cooperation_class')
      .distinct(true)
      .getRawMany();

    const sessionID = await this.commEventsRepo
      .createQueryBuilder('comm_event')
      .select('comm_event.session_id')
      .distinct(true)
      .getRawMany();

    const communicationClass = await this.commEventsRepo
      .createQueryBuilder('comm_event')
      .select('comm_event.communication_class')
      .distinct(true)
      .getRawMany();

    const messageType = await this.commEventsRepo
      .createQueryBuilder('comm_event')
      .select('comm_event.message_type')
      .distinct(true)
      .getRawMany();

    const communicationMethod = await this.commEventsRepo
      .createQueryBuilder('comm_event')
      .select('comm_event.method')
      .distinct(true)
      .getRawMany();

    return {
      nodeList: nodeMap,
      eventStatus: enumToKeyValue(EventStatus),
      cooperationClass: convertJsonArrayToObject(
        cooperationClass,
        'cooperation_class',
      ),
      sessionID: convertJsonArrayToObject(sessionID, 'session_id'),
      communicationClass: convertJsonArrayToObject(
        communicationClass,
        'communication_class',
      ),
      communicationMethod: convertJsonArrayToObject(
        communicationMethod,
        'comm_event_method',
      ),
      messageType: convertJsonArrayToObject(messageType, 'message_type'),
    };
  }

  async getEdgeStatus(data: IAvailEvent) {
    const event = await this.eventService.parseDataToAvailEvent(data);
    const result = await this.eventService.saveEvent(1, event);
    if (result.status == 2) {
      const notification = { nodeID: data.nodeID, detail: event.detail };
      this.gatewayService.server.emit('notification', notification);
    }
  }

  async getEdgeMessageList(data: ICommEventList) {
    const events = await this.eventService.parseDataToCommEvent(data);
    const result = await this.eventService.saveEvent(2, events);
  }

  getEdgeKeepAlive(data: IKeepAliveMessage) {
    return this.eventService.parseDataToKeepAlive(data);
  }

  async getEdgeScannerEvent(fileScannerEvent: Express.Multer.File) {
    try {
      const fileContent = fileScannerEvent.buffer.toString('utf-8')

      const scannerEvents: ScannerEventDTO[] = this.parseFileContentToScannerEventDTO(fileContent);

      const externalServerUrl = process.env.EXTERNAL_SERVER_URL || 'http://localhost:4000'

      for (const scannerEvent of scannerEvents) {
        const event = await this.eventService.parseDataToScannerEvent(scannerEvent);
        console.log(event);
        const savedEvent = await this.eventService.saveEvent(3, event);
        if (savedEvent.status == 2) {
          const notification = { nodeID: 'RF Scanner', detail: event.detail };
              
          this.gatewayService.server.emit('notification', notification);
        }
        await axios.post(externalServerUrl, savedEvent)
      }
      return { success: true, message: 'Event processed and published successfully'}
    } catch (error) {
      console.error('Error processing edge scanner event::', error.message);
      throw new Error('Failed to process edge scanner event');
    }
  }

  private parseFileContentToScannerEventDTO(content: string): ScannerEventDTO[] {
    const lines = content.split('\n').map(line => line.trim()).filter(line => line);
  
    if (!lines[0].startsWith('[RF]') || !lines.includes('[END]')) {
      throw new Error('Invalid file format: Missing required metadata or [END] marker');
    }
  
    const metadataLine = lines[0];
    const [, date, time, signalNum] = metadataLine.split(/\s+/);
    const timeStamp = formatTimestamp(date, time);
  
    const signalLines = lines.slice(1, lines.indexOf('[END]'));
    const scannerEvents: ScannerEventDTO[] = [];
  
    for (const line of signalLines) {
      const fields = line.split(/\s+/);
  
      if (fields.length !== 8) {
        console.warn(`Skipping invalid line: ${line}`);
        continue;
      }
  
      const [signalId, setNum, centerFreq, bandwidth, elevation, azimuth, signalPower, signalClass] =
        fields.map(Number);
  
      scannerEvents.push({
        signalNum: parseInt(signalNum, 10),
        signalId,
        setNum,
        centerFreq,
        bandwidth,
        elevation,
        azimuth,
        signalPower,
        signalClass,
        timeStamp,
      });
    }
  
    return scannerEvents;
  }
  

  // changeAvailEventProp(prop: AvailEventPropGenInf) {
  //   this.AvailEventProp = prop;
  // }

  // changeCommEventProp(prop: CommEventPropGenInf) {
  //   this.CommEventProp = prop;
  // }

  // async genAvailEvents() {
  //   if (!this.AvailEventProp) {
  //     return;
  //   }

  //   const nodes = this.AvailEventProp.nodes;
  //   const eventList = nodes.map((node) => ({
  //     timeStamp: getUnixCurrentTime(),
  //     nodeID: node.nodeID,
  //     rsuName: node.rsuName,
  //     cpuUsage: randomNumber(
  //       this.AvailEventProp.cpuUsage.min,
  //       this.AvailEventProp.cpuUsage.max,
  //     ),
  //     cpuTemperature: randomNumber(
  //       this.AvailEventProp.cpuTemperature.min,
  //       this.AvailEventProp.cpuTemperature.max,
  //     ),
  //     ramUsage: randomNumber(
  //       this.AvailEventProp.ramUsage.min,
  //       this.AvailEventProp.ramUsage.max,
  //     ),
  //     diskUsage: randomNumber(
  //       this.AvailEventProp.diskUsage.min,
  //       this.AvailEventProp.diskUsage.max,
  //     ),
  //     rsuConnection: true,
  //     networkSpeed: randomNumber(
  //       this.AvailEventProp.networkSpeed.min,
  //       this.AvailEventProp.networkSpeed.max,
  //     ),
  //     networkUsage: randomNumber(
  //       this.AvailEventProp.networkUsage.min,
  //       this.AvailEventProp.networkUsage.max,
  //     ),
  //     latitude: node.latitude,
  //     longitude: node.longitude,
  //   }));
  //   return eventList;
  // }

  // async genCommEvents() {
  //   if (!this.CommEventProp) {
  //     return;
  //   }

  //   const eventList = [];
  //   const nodes = this.CommEventProp.nodes;
  //   const communicationType = randomChoice(['broadcasting', 'unicasting']);
  //   if (communicationType === 'broadcasting') {
  //     const node = randomChoice(nodes);
  //     eventList.push({
  //       communicationClass: 'send',
  //       timeStamp: getUnixCurrentTime(),
  //       nodeID: node.nodeID,
  //       rsuName: node.rsuName,
  //       cooperationClass: randomChoice(this.CommEventProp.cooperationClass),
  //       sessionID: randomNumber(
  //         this.CommEventProp.sessionID.min,
  //         this.CommEventProp.sessionID.max,
  //       ),
  //       communicationType: communicationType,
  //       senderNodeID: node.nodeID,
  //       receiverNodeID: 'B',
  //       messageType: randomChoice(this.CommEventProp.messageType),
  //       messageData: '000000101010101001',
  //     });
  //   } else {
  //     const senderNode = randomChoice(nodes);
  //     const receiverNode = randomChoice([
  //       ...nodes.filter((item) => item !== senderNode),
  //     ]);

  //     const sendEvent = {
  //       communicationClass: 'send',
  //       timeStamp: getUnixCurrentTime(),
  //       nodeID: senderNode.nodeID,
  //       rsuName: senderNode.rsuName,
  //       cooperationClass: randomChoice(this.CommEventProp.cooperationClass),
  //       sessionID: randomNumber(
  //         this.CommEventProp.sessionID.min,
  //         this.CommEventProp.sessionID.max,
  //       ),
  //       communicationType: communicationType,
  //       senderNodeID: senderNode.nodeID,
  //       receiverNodeID: receiverNode.nodeID,
  //       messageType: randomChoice(this.CommEventProp.messageType),
  //       messageData: '000000101010101001',
  //     };

  //     const receiveEvent = {
  //       communicationClass: 'receive',
  //       timeStamp: getUnixCurrentTime(),
  //       nodeID: receiverNode.nodeID,
  //       rsuName: receiverNode.rsuName,
  //       cooperationClass: null,
  //       sessionID: null,
  //       communicationType: communicationType,
  //       senderNodeID: senderNode.nodeID,
  //       receiverNodeID: receiverNode.nodeID,
  //       messageType: randomChoice(this.CommEventProp.messageType),
  //       messageData: '000000101010101001',
  //     };

  //     eventList.push([sendEvent, receiveEvent]);
  //   }
  //   return eventList;
  // }
}

const convertJsonArrayToObject = (jsonArray: any[], key: string) => {
  const jsonObject: { [key: string]: string } = {};

  for (const item of jsonArray) {
    const value = item[key];
    if (value) jsonObject[value] = value;
  }

  return jsonObject;
};
