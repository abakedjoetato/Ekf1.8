# Improved final status message to show mode and events processed
"""Update send_log_embeds to use server-specific channels"""
"""
Emerald's Killfeed - Unified Log Parser System
Consolidated from fragmented parsers with complete mission normalization
PHASE 1 & 2 Complete Implementation
"""

import asyncio
import logging
import os
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple

import aiofiles
import discord
import asyncssh
from discord.ext import commands

from bot.utils.embed_factory import EmbedFactory

logger = logging.getLogger(__name__)

class UnifiedLogParser:
    """
    UNIFIED LOG PARSER - Consolidates all log parsing functionality
    - Replaces log_parser.py, intelligent_log_parser.py, connection_parser.py
    - Implements complete mission normalization from actual Deadside.log analysis
    - Uses EmbedFactory for all outputs
    - Maintains guild isolation logic
    """

    def __init__(self, bot):
        self.bot = bot
        # All state dictionaries use guild_server keys for complete isolation
        self.last_log_position: Dict[str, int] = {}  # {guild_id}_{server_id} -> position
        self.log_file_hashes: Dict[str, str] = {}    # {guild_id}_{server_id} -> hash
        self.player_sessions: Dict[str, Dict[str, Any]] = {}  # {guild_id}_{player_id} -> session_data
        self.server_status: Dict[str, Dict[str, Any]] = {}    # {guild_id}_{server_id} -> status
        self.sftp_connections: Dict[str, asyncssh.SSHClientConnection] = {}  # {guild_id}_{server_id}_{host}_{port} -> connection
        self.file_states: Dict[str, Dict[str, Any]] = {}      # {guild_id}_{server_id} -> file_state
        self.player_lifecycle: Dict[str, Dict[str, Any]] = {} # {guild_id}_{player_id} -> lifecycle_data

        # Comprehensive log patterns from actual Deadside.log analysis
        self.patterns = self._compile_unified_patterns()

        # Complete mission normalization from real log data
        self.mission_mappings = self._get_complete_mission_mappings()

        # Load persistent state on startup
        asyncio.create_task(self._load_persistent_state())

    def _compile_unified_patterns(self) -> Dict[str, re.Pattern]:
        """Compile all log patterns from actual Deadside.log analysis"""
        return {
            # SERVER LIFECYCLE
            'log_rotation': re.compile(r'^Log file open, (\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'),
            'server_startup': re.compile(r'LogWorld: Bringing World.*up for play.*at (\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})'),
            'world_loaded': re.compile(r'LogLoad: Took .* seconds to LoadMap.*World_0'),
            'server_max_players': re.compile(r'playersmaxcount=(\d+)', re.IGNORECASE),

            # PLAYER CONNECTION LIFECYCLE - From actual log patterns
            'player_queue_join': re.compile(r'LogNet: Join request: /Game/Maps/world_\d+/World_\d+\?.*eosid=\|([a-f0-9]+).*Name=([^&\?]+)', re.IGNORECASE),
            'player_beacon_join': re.compile(r'LogBeacon: Beacon Join SFPSOnlineBeaconClient EOS:\|([a-f0-9]+)', re.IGNORECASE),
            'player_registered': re.compile(r'LogOnline: Warning: Player \|([a-f0-9]+) successfully registered!', re.IGNORECASE),
            'player_disconnect': re.compile(r'UChannel::Close: Sending CloseBunch.*UniqueId: EOS:\|([a-f0-9]+)', re.IGNORECASE),
            'player_cleanup': re.compile(r'UNetConnection::Close: Connection cleanup.*UniqueId: EOS:\|([a-f0-9]+)', re.IGNORECASE),

            # MISSION EVENTS - Patterns from actual Deadside.log
            'mission_respawn': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) will respawn in (\d+)', re.IGNORECASE),
            'mission_state_change': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to ([A-Z_]+)', re.IGNORECASE),
            'mission_ready': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to READY', re.IGNORECASE),
            'mission_initial': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to INITIAL', re.IGNORECASE),
            'mission_in_progress': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to IN_PROGRESS', re.IGNORECASE),
            'mission_completed': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to COMPLETED', re.IGNORECASE),

            # VEHICLE EVENTS
            'vehicle_spawn': re.compile(r'LogSFPS: \[ASFPSGameMode::NewVehicle_Add\] Add vehicle (BP_SFPSVehicle_[A-Za-z0-9_]+)', re.IGNORECASE),
            'vehicle_delete': re.compile(r'LogSFPS: \[ASFPSGameMode::NewVehicle_Del\] Del vehicle (BP_SFPSVehicle_[A-Za-z0-9_]+)', re.IGNORECASE),

            # TIMESTAMP EXTRACTION
            'timestamp': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\]')
        }

    def _get_complete_mission_mappings(self) -> Dict[str, str]:
        """
        Complete mission normalization from actual Deadside.log analysis
        Maps all discovered mission IDs to proper readable names
        """
        return {
            # Airport Missions
            'GA_Airport_mis_01_SFPSACMission': 'Airport Mission #1',
            'GA_Airport_mis_02_SFPSACMission': 'Airport Mission #2', 
            'GA_Airport_mis_03_SFPSACMission': 'Airport Mission #3',
            'GA_Airport_mis_04_SFPSACMission': 'Airport Mission #4',

            # Settlement Missions
            'GA_Beregovoy_Mis1': 'Beregovoy Settlement Mission',
            'GA_Settle_05_ChernyLog_Mis1': 'Cherny Log Settlement Mission',
            'GA_Settle_09_Mis_1': 'Settlement Mission #9',

            # Military Base Missions
            'GA_Military_02_Mis1': 'Military Base Mission #2',
            'GA_Military_03_Mis_01': 'Military Base Mission #3',
            'GA_Military_04_Mis1': 'Military Base Mission #4',
            'GA_Military_04_Mis_2': 'Military Base Mission #4B',

            # Industrial Missions
            'GA_Ind_01_m1': 'Industrial Zone Mission #1',
            'GA_Ind_02_Mis_1': 'Industrial Zone Mission #2',
            'GA_PromZone_6_Mis_1': 'Industrial Zone Mission #6',
            'GA_PromZone_Mis_01': 'Industrial Zone Mission A',
            'GA_PromZone_Mis_02': 'Industrial Zone Mission B',

            # Chemical Plant Missions
            'GA_KhimMash_Mis_01': 'Chemical Plant Mission #1',
            'GA_KhimMash_Mis_02': 'Chemical Plant Mission #2',

            # City Missions
            'GA_Kamensk_Ind_3_Mis_1': 'Kamensk Industrial Mission',
            'GA_Kamensk_Mis_1': 'Kamensk City Mission #1',
            'GA_Kamensk_Mis_2': 'Kamensk City Mission #2', 
            'GA_Kamensk_Mis_3': 'Kamensk City Mission #3',
            'GA_Krasnoe_Mis_1': 'Krasnoe City Mission',
            'GA_Vostok_Mis_1': 'Vostok City Mission',

            # Special Locations
            'GA_Bunker_01_Mis1': 'Underground Bunker Mission',
            'GA_Lighthouse_02_Mis1': 'Lighthouse Mission #2',
            'GA_Elevator_Mis_1': 'Elevator Complex Mission #1',
            'GA_Elevator_Mis_2': 'Elevator Complex Mission #2',

            # Resource Missions
            'GA_Sawmill_01_Mis1': 'Sawmill Mission #1',
            'GA_Sawmill_02_1_Mis1': 'Sawmill Mission #2A',
            'GA_Sawmill_03_Mis_01': 'Sawmill Mission #3',
            'GA_Bochki_Mis_1': 'Barrel Storage Mission',
            'GA_Dubovoe_0_Mis_1': 'Dubovoe Resource Mission',
        }

    def normalize_mission_name(self, mission_id: str) -> str:
        """
        Normalize mission ID to readable name
        Returns proper name from mapping or generates descriptive fallback
        """
        if mission_id in self.mission_mappings:
            return self.mission_mappings[mission_id]

        # Generate intelligent fallback for unmapped missions
        if '_Airport_' in mission_id:
            return f"Airport Mission ({mission_id.split('_')[-1]})"
        elif '_Military_' in mission_id:
            return f"Military Mission ({mission_id.split('_')[-1]})"
        elif '_Settle_' in mission_id:
            return f"Settlement Mission ({mission_id.split('_')[-1]})"
        elif '_Ind_' in mission_id or '_PromZone_' in mission_id:
            return f"Industrial Mission ({mission_id.split('_')[-1]})"
        elif '_KhimMash_' in mission_id:
            return f"Chemical Plant Mission ({mission_id.split('_')[-1]})"
        elif '_Bunker_' in mission_id:
            return f"Bunker Mission ({mission_id.split('_')[-1]})"
        elif '_Sawmill_' in mission_id:
            return f"Sawmill Mission ({mission_id.split('_')[-1]})"
        else:
            # Extract readable parts from mission ID
            parts = mission_id.replace('GA_', '').replace('_Mis', '').replace('_mis', '').split('_')
            readable_parts = [part.capitalize() for part in parts if part.isalpha()]
            if readable_parts:
                return f"{' '.join(readable_parts)} Mission"
            else:
                return f"Special Mission ({mission_id})"

    def get_mission_level(self, mission_id: str) -> int:
        """Determine mission difficulty level based on type"""
        if any(keyword in mission_id.lower() for keyword in ['military', 'bunker', 'khimmash']):
            return 5  # High tier
        elif any(keyword in mission_id.lower() for keyword in ['airport', 'promzone', 'kamensk']):
            return 4  # High-medium tier
        elif any(keyword in mission_id.lower() for keyword in ['ind_', 'industrial']):
            return 3  # Medium tier
        elif any(keyword in mission_id.lower() for keyword in ['sawmill', 'lighthouse', 'elevator']):
            return 2  # Low-medium tier
        else:
            return 1  # Low tier

    async def process_mission_event(self, guild_id: str, mission_id: str, state: str, respawn_time: Optional[int] = None) -> Optional[discord.Embed]:
        """
        Process mission event and create normalized embed
        Uses EmbedFactory for consistent formatting
        """
        try:
            normalized_name = self.normalize_mission_name(mission_id)
            mission_level = self.get_mission_level(mission_id)

            # Create embed using EmbedFactory
            if state == 'READY':
                embed = EmbedFactory.create_mission_embed(
                    title="🎯 Mission Available",
                    description=f"**{normalized_name}** is now available for completion",
                    mission_id=mission_id,
                    level=mission_level,
                    state="READY",
                    color=0x00FF00
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")
            elif state == 'IN_PROGRESS':
                embed = EmbedFactory.create_mission_embed(
                    title="⚔️ Mission In Progress", 
                    description=f"**{normalized_name}** is currently being completed",
                    mission_id=mission_id,
                    level=mission_level,
                    state="IN_PROGRESS",
                    color=0xFFAA00
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")
            elif state == 'COMPLETED':
                embed = EmbedFactory.create_mission_embed(
                    title="✅ Mission Completed",
                    description=f"**{normalized_name}** has been completed successfully",
                    mission_id=mission_id,
                    level=mission_level,
                    state="COMPLETED",
                    color=0x0099FF
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")
            elif respawn_time:
                embed = EmbedFactory.create_mission_embed(
                    title="🔄 Mission Respawning",
                    description=f"**{normalized_name}** will respawn in {respawn_time} seconds",
                    mission_id=mission_id,
                    level=mission_level,
                    state="RESPAWN",
                    respawn_time=respawn_time,
                    color=0x888888
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")
            else:
                embed = EmbedFactory.create_mission_embed(
                    title="📋 Mission Update",
                    description=f"**{normalized_name}** state: {state}",
                    mission_id=mission_id,
                    level=mission_level,
                    state=state,
                    color=0x666666
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")

            return embed

        except Exception as e:
            logger.error(f"Failed to process mission event: {e}")
            return None

    async def process_player_connection(self, guild_id: str, player_id: str, player_name: str, event_type: str) -> Optional[discord.Embed]:
        """
        Process player connection event with unified lifecycle tracking
        Uses EmbedFactory for consistent formatting
        """
        try:
            # Update player session tracking
            session_key = f"{guild_id}_{player_id}"

            if event_type == 'joined':
                # Track player join
                self.player_sessions[session_key] = {
                    'player_id': player_id,
                    'player_name': player_name,
                    'guild_id': guild_id,
                    'joined_at': datetime.now(timezone.utc).isoformat(),
                    'status': 'online'
                }

                # Update voice channel with new player count
                await self.update_voice_channel(guild_id)

                embed = EmbedFactory.create_connection_embed(
                    title="🟢 Player Connected",
                    description=f"**{player_name}** has joined the server",
                    player_name=player_name,
                    player_id=player_id,
                    color=0x00FF00
                )
                # Add metadata for channel routing
                embed.set_footer(text="Connection Event • Powered by Discord.gg/EmeraldServers")

            elif event_type == 'disconnected':
                # Track player disconnect
                if session_key in self.player_sessions:
                    self.player_sessions[session_key]['status'] = 'offline'
                    self.player_sessions[session_key]['left_at'] = datetime.now(timezone.utc).isoformat()

                # Update voice channel with new player count
                await self.update_voice_channel(guild_id)

                embed = EmbedFactory.create_connection_embed(
                    title="🔴 Player Disconnected", 
                    description=f"**{player_name}** has left the server",
                    player_name=player_name,
                    player_id=player_id,
                    color=0xFF0000
                )
                # Add metadata for channel routing
                embed.set_footer(text="Connection Event • Powered by Discord.gg/EmeraldServers")
            else:
                return None

            return embed

        except Exception as e:
            logger.error(f"Failed to process player connection: {e}")
            return None

    async def parse_log_content(self, content: str, guild_id: str, server_id: str, cold_start_mode: bool = False) -> List[discord.Embed]:
        """
        Parse log content and return list of embeds for events
        Unified processing of all log events with incremental tracking
        """
        embeds = []
        lines = content.splitlines()
        total_lines = len(lines)

        # Check for incremental processing (hot start)
        server_key = f"{guild_id}_{server_id}"
        stored_state = self.file_states.get(server_key, {})
        last_processed = stored_state.get('line_count', 0)

        # Only process new lines in hot start mode
        if last_processed > 0 and last_processed < total_lines:
            new_lines = lines[last_processed:]
            logger.info(f"🔥 HOT START: Processing {len(new_lines)} new lines ({last_processed+1} to {total_lines})")
            lines = new_lines
        elif last_processed >= total_lines:
            logger.info("📊 No new lines to process")
            return embeds
        else:
            # First run or file reset - process all lines
            logger.info(f"🆕 PROCESSING ALL LINES: {total_lines} total lines")

        # Update file state BEFORE processing to prevent reprocessing
        self.file_states[server_key] = {
            'line_count': total_lines,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }

        # Save persistent state immediately
        await self._save_persistent_state()

        processed_events = 0
        for line_idx, line in enumerate(lines):
            try:
                # Mission events
                for pattern_name, pattern in self.patterns.items():
                    if pattern_name.startswith('mission_'):
                        match = pattern.search(line)
                        if match:
                            if pattern_name == 'mission_respawn':
                                mission_id, respawn_time = match.groups()
                                embed = await self.process_mission_event(
                                    guild_id, mission_id, 'RESPAWN', int(respawn_time)
                                )
                            elif pattern_name == 'mission_state_change':
                                mission_id, state = match.groups()
                                embed = await self.process_mission_event(
                                    guild_id, mission_id, state
                                )
                            else:
                                continue

                            if embed:
                                processed_events += 1
                                if not cold_start_mode:
                                    embeds.append(embed)
                                    logger.info(f"📋 Processed {pattern_name}: {mission_id if 'mission_id' in locals() else 'event'}")
                                else:
                                    logger.debug(f"📋 Processed {pattern_name}: {mission_id if 'mission_id' in locals() else 'event'} (embed skipped)")
                            else:
                                logger.debug(f"📋 Failed to process {pattern_name}: {mission_id if 'mission_id' in locals() else 'event'}")

                            # Safety check - prevent massive embed generation
                            if not cold_start_mode and processed_events > len(lines) * 2:
                                logger.error(f"⚠️ SAFETY BREAK: Generated {processed_events} events from {len(lines)} lines - stopping")
                                break

                # Player connection events - with name extraction
                player_queue_join = self.patterns['player_queue_join'].search(line)
                if player_queue_join:
                    player_id, player_name = player_queue_join.groups()
                    # Store name for later use when player registers
                    player_key = f"{guild_id}_{player_id}"
                    self.player_lifecycle[player_key] = {
                        'name': player_name,
                        'queue_joined': datetime.now(timezone.utc).isoformat()
                    }

                player_registered = self.patterns['player_registered'].search(line)
                if player_registered:
                    player_id = player_registered.group(1)
                    player_key = f"{guild_id}_{player_id}"

                    # Get player name from lifecycle tracking
                    player_name = "Unknown Player"
                    if player_key in self.player_lifecycle:
                        player_name = self.player_lifecycle[player_key].get('name', 'Unknown Player')

                    # Always process player connection data
                    embed = await self.process_player_connection(
                        guild_id, player_id, player_name, 'joined'
                    )
                    
                    if embed:
                        processed_events += 1
                        if not cold_start_mode:
                            embeds.append(embed)
                        else:
                            logger.debug(f"👤 Processed player join: {player_name} (embed skipped)")
                    else:
                        logger.debug(f"👤 Failed to process player join: {player_name}")

                player_disconnect = self.patterns['player_disconnect'].search(line)
                if player_disconnect:
                    player_id = player_disconnect.group(1)
                    player_key = f"{guild_id}_{player_id}"

                    # Get player name from session tracking
                    session_key = f"{guild_id}_{player_id}"
                    player_name = "Unknown Player"
                    if session_key in self.player_sessions:
                        player_name = self.player_sessions[session_key].get('player_name', 'Unknown Player')
                    elif player_key in self.player_lifecycle:
                        player_name = self.player_lifecycle[player_key].get('name', 'Unknown Player')

                    # Always process player disconnect data
                    embed = await self.process_player_connection(
                        guild_id, player_id, player_name, 'disconnected'
                    )
                    
                    if embed:
                        processed_events += 1
                        if not cold_start_mode:
                            embeds.append(embed)
                        else:
                            logger.debug(f"👤 Processed player disconnect: {player_name} (embed skipped)")
                    else:
                        logger.debug(f"👤 Failed to process player disconnect: {player_name}")

                # Safety check after each line (more lenient for cold start)
                max_events = len(lines) * 5 if cold_start_mode else len(lines) * 2
                if processed_events > max_events:
                    logger.error(f"⚠️ SAFETY BREAK: Generated {processed_events} events from {len(lines)} lines - stopping processing")
                    break

            except Exception as e:
                logger.error(f"Error processing log line: {e}")
                continue

        # Final status logging
        if cold_start_mode:
            logger.info(f"🔍 Cold start completed: tracked {processed_events} events from {len(lines)} lines (no embeds generated)")
        else:
            logger.info(f"🔍 Parser completed: found {len(embeds)} events from {len(lines)} new lines")

        return embeds

    async def _load_persistent_state(self):
        """Load persistent state from database"""
        try:
            self.file_states = {}

            if hasattr(self.bot, 'db_manager') and self.bot.db_manager:
                # Load file states from database
                state_doc = await self.bot.db_manager.db['parser_state'].find_one({'_id': 'unified_parser_state'})

                if state_doc and 'file_states' in state_doc:
                    self.file_states = state_doc['file_states']
                    logger.info(f"Loaded persistent state for unified parser - {len(self.file_states)} server states")
                else:
                    logger.info("No persistent state found, starting fresh")
            else:
                logger.info("Database not available for state loading")

        except Exception as e:
            logger.error(f"Failed to load persistent state: {e}")
            self.file_states = {}

    async def _save_persistent_state(self):
        """Save persistent state for incremental processing"""
        try:
            if hasattr(self.bot, 'db_manager') and self.bot.db_manager:
                # Save file states to database
                state_doc = {
                    '_id': 'unified_parser_state',
                    'file_states': self.file_states,
                    'last_updated': datetime.now(timezone.utc).isoformat()
                }

                await self.bot.db_manager.db['parser_state'].replace_one(
                    {'_id': 'unified_parser_state'},
                    state_doc,
                    upsert=True
                )
                logger.debug(f"Persistent state saved - {len(self.file_states)} server states")
            else:
                logger.debug("Database not available for state persistence")
        except Exception as e:
            logger.error(f"Failed to save persistent state: {e}")

    def reset_file_states(self, server_key: Optional[str] = None, guild_id: Optional[int] = None, reset_all_tracking: bool = False):
        """Reset file states to force cold start on next run"""
        if server_key:
            if server_key in self.file_states:
                del self.file_states[server_key]
                logger.info(f"Reset file state for {server_key}")
            
            if reset_all_tracking:
                # Reset player tracking for this server
                guild_id_from_key = server_key.split('_')[0]
                guild_prefix = f"{guild_id_from_key}_"
                
                # Remove player sessions for this guild
                sessions_to_remove = [k for k in self.player_sessions.keys() if k.startswith(guild_prefix)]
                for key in sessions_to_remove:
                    del self.player_sessions[key]
                
                # Remove player lifecycle data for this guild
                lifecycle_to_remove = [k for k in self.player_lifecycle.keys() if k.startswith(guild_prefix)]
                for key in lifecycle_to_remove:
                    del self.player_lifecycle[key]
                
                # Remove server status for this server
                if server_key in self.server_status:
                    del self.server_status[server_key]
                
                logger.info(f"Reset all tracking states for {server_key}")
                
        elif guild_id:
            # Reset all states for a specific guild
            guild_prefix = f"{guild_id}_"
            keys_to_remove = [k for k in self.file_states.keys() if k.startswith(guild_prefix)]
            for key in keys_to_remove:
                del self.file_states[key]
            
            if reset_all_tracking:
                # Reset all player tracking for this guild
                sessions_to_remove = [k for k in self.player_sessions.keys() if k.startswith(guild_prefix)]
                for key in sessions_to_remove:
                    del self.player_sessions[key]
                
                lifecycle_to_remove = [k for k in self.player_lifecycle.keys() if k.startswith(guild_prefix)]
                for key in lifecycle_to_remove:
                    del self.player_lifecycle[key]
                
                status_to_remove = [k for k in self.server_status.keys() if k.startswith(guild_prefix)]
                for key in status_to_remove:
                    del self.server_status[key]
                
                logger.info(f"Reset all tracking states for guild {guild_id}")
            
            logger.info(f"Reset all file states for guild {guild_id} ({len(keys_to_remove)} servers)")
        else:
            self.file_states.clear()
            if reset_all_tracking:
                self.player_sessions.clear()
                self.player_lifecycle.clear()
                self.server_status.clear()
                self.last_log_position.clear()
                self.log_file_hashes.clear()
                logger.info("Reset all file states and tracking data")
            else:
                logger.info("Reset all file states")

    def get_guild_server_state(self, guild_id: int, server_id: str) -> Dict[str, Any]:
        """Get isolated state for a specific guild-server combination"""
        server_key = f"{guild_id}_{server_id}"
        return {
            'file_state': self.file_states.get(server_key, {}),
            'server_status': self.server_status.get(server_key, {}),
            'active_players': [
                session for session_key, session in self.player_sessions.items()
                if session_key.startswith(f"{guild_id}_") and session.get('status') == 'online'
            ],
            'sftp_connected': any(
                conn_key.startswith(f"{guild_id}_{server_id}_") 
                for conn_key in self.sftp_connections.keys()
            )
        }

    def cleanup_guild_state(self, guild_id: int):
        """Clean up all state for a guild (when bot leaves guild)"""
        guild_prefix = f"{guild_id}_"

        # Clean up all state dictionaries
        for state_dict in [self.file_states, self.player_sessions, self.server_status, 
                          self.player_lifecycle, self.last_log_position, self.log_file_hashes]:
            keys_to_remove = [k for k in state_dict.keys() if k.startswith(guild_prefix)]
            for key in keys_to_remove:
                del state_dict[key]

        # Close SFTP connections for this guild
        conn_keys_to_remove = [k for k in self.sftp_connections.keys() if k.startswith(guild_prefix)]
        for conn_key in conn_keys_to_remove:
            try:
                self.sftp_connections[conn_key].close()
            except:
                pass
            del self.sftp_connections[conn_key]

        logger.info(f"Cleaned up all state for guild {guild_id}")

    def get_parser_status(self) -> Dict[str, Any]:
        """Get parser status for debugging"""
        active_sessions = sum(1 for session in self.player_sessions.values() if session.get('status') == 'online')

        return {
            'active_sessions': active_sessions,
            'total_tracked_servers': len(self.file_states),
            'sftp_connections': len(self.sftp_connections),
            'file_states': {k: v for k, v in self.file_states.items()},
            'connection_status': 'healthy' if self.sftp_connections else 'no_connections'
        }

    async def update_voice_channel(self, guild_id: str):
        """Update voice channel player count with current online players"""
        try:
            guild_id_int = int(guild_id) if isinstance(guild_id, str) else guild_id
            
            # Count active players for this guild
            guild_prefix = f"{guild_id}_"
            active_players = sum(1 for session_key, session in self.player_sessions.items() 
                               if session_key.startswith(guild_prefix) and session.get('status') == 'online')
            
            # Get guild configuration for voice channel
            guild_config = await self.bot.db_manager.get_guild(guild_id_int)
            if not guild_config:
                return
                
            # Check for voice channel configuration
            voice_channel_id = guild_config.get('channels', {}).get('voice_count')
            if not voice_channel_id:
                # Check server-specific voice channels
                server_channels = guild_config.get('server_channels', {})
                for server_id, channels in server_channels.items():
                    if 'voice_count' in channels:
                        voice_channel_id = channels['voice_count']
                        break
                        
            if not voice_channel_id:
                return
                
            # Update voice channel name with player count
            try:
                guild = self.bot.get_guild(guild_id_int)
                if guild:
                    voice_channel = guild.get_channel(voice_channel_id)
                    if voice_channel and voice_channel.type == discord.ChannelType.voice:
                        new_name = f"🟢 Players Online: {active_players}"
                        if voice_channel.name != new_name:
                            await voice_channel.edit(name=new_name)
                            logger.debug(f"Updated voice channel to show {active_players} players online")
            except Exception as e:
                logger.warning(f"Failed to update voice channel: {e}")
                
        except Exception as e:
            logger.error(f"Error updating voice channel for guild {guild_id}: {e}")

    async def get_server_channel(self, guild_id: int, server_id: str, channel_type: str) -> Optional[int]:
        """Get server-specific channel ID with enhanced fallback logic"""
        try:
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                logger.debug(f"No guild config found for guild {guild_id}")
                return None

            server_channels = guild_config.get('server_channels', {})

            # Priority 1: Server-specific channel
            if server_id in server_channels:
                channel_id = server_channels[server_id].get(channel_type)
                if channel_id:
                    logger.debug(f"Using server-specific {channel_type} channel {channel_id} for server {server_id}")
                    return channel_id

            # Priority 2: Default server channel (from /setchannel with default server)
            if 'default' in server_channels:
                channel_id = server_channels['default'].get(channel_type)
                if channel_id:
                    logger.debug(f"Using default {channel_type} channel {channel_id} for server {server_id}")
                    return channel_id

            # Priority 3: Fallback to killfeed channel if no specific channel type is set
            # This ensures messages go somewhere rather than being lost
            if channel_type != 'killfeed':
                fallback_channel = None
                
                # Try server-specific killfeed first
                if server_id in server_channels:
                    fallback_channel = server_channels[server_id].get('killfeed')
                
                # Try default killfeed
                if not fallback_channel and 'default' in server_channels:
                    fallback_channel = server_channels['default'].get('killfeed')
                
                # Try legacy killfeed
                if not fallback_channel:
                    fallback_channel = guild_config.get('channels', {}).get('killfeed')
                
                if fallback_channel:
                    logger.debug(f"Using killfeed channel {fallback_channel} as fallback for {channel_type} (server {server_id})")
                    return fallback_channel

            # Priority 4: Legacy fallback to old channel structure
            legacy_channel_id = guild_config.get('channels', {}).get(channel_type)
            if legacy_channel_id:
                logger.debug(f"Using legacy {channel_type} channel {legacy_channel_id} for server {server_id}")
                return legacy_channel_id

            # Priority 5: Legacy killfeed fallback
            if channel_type != 'killfeed':
                legacy_killfeed = guild_config.get('channels', {}).get('killfeed')
                if legacy_killfeed:
                    logger.debug(f"Using legacy killfeed channel {legacy_killfeed} as fallback for {channel_type} (server {server_id})")
                    return legacy_killfeed

            logger.debug(f"No {channel_type} channel configured for guild {guild_id}, server {server_id}")
            return None

        except Exception as e:
            logger.error(f"Failed to get {channel_type} channel for guild {guild_id}, server {server_id}: {e}")
            return None

    async def send_log_embeds(self, guild_id: int, server_id: str, embeds_data: List[Dict[str, Any]]):
        """Send log embeds to appropriate channels based on event type with server-specific routing"""
        try:
            if not embeds_data:
                return

            # Channel mapping for different event types
            channel_mapping = {
                'mission_event': 'events',
                'airdrop_event': 'events', 
                'helicrash_event': 'events',
                'trader_event': 'events',
                'vehicle_event': 'events',
                'player_connection': 'connections',
                'player_disconnection': 'connections'
            }

            for embed_data in embeds_data:
                embed_type = embed_data.get('type')
                channel_type = channel_mapping.get(embed_type)

                if not channel_type:
                    logger.warning(f"Unknown embed type: {embed_type}")
                    continue

                # Get server-specific channel with fallback
                channel_id = await self.get_server_channel(guild_id, server_id, channel_type)
                if not channel_id:
                    logger.debug(f"No {channel_type} channel configured for guild {guild_id}, server {server_id}")
                    continue

                channel = self.bot.get_channel(channel_id)
                if not channel:
                    logger.warning(f"Channel {channel_id} not found for {channel_type}")
                    continue

                try:
                    embed_dict = embed_data.get('embed')
                    if embed_dict:
                        await channel.send(embed=discord.Embed.from_dict(embed_dict))
                        logger.info(f"Sent {channel_type} event to {channel.name} (ID: {channel_id})")
                except Exception as e:
                    logger.error(f"Failed to send {channel_type} event to channel {channel_id}: {e}")

        except Exception as e:
            logger.error(f"Failed to send log embeds: {e}")

    def _determine_channel_type(self, embed: discord.Embed) -> Optional[str]:
        """Determine which channel type an embed should go to based on its content"""
        if not embed.title:
            return None

        title_lower = embed.title.lower()

        # Map embed types to channel types
        if any(keyword in title_lower for keyword in ['airdrop', 'crate']):
            return 'events'
        elif any(keyword in title_lower for keyword in ['mission', 'objective']):
            return 'events'
        elif any(keyword in title_lower for keyword in ['helicopter', 'heli', 'crash']):
            return 'events'
        elif any(keyword in title_lower for keyword in ['connect', 'disconnect', 'join', 'left']):
            return 'connections'
        elif any(keyword in title_lower for keyword in ['bounty']):
            return 'bounties'
        else:
            # Default to events for most server activities
            return 'events'

    def _determine_embed_type_from_title(self, title: str) -> str:
        """Determine embed type from title string"""
        title_lower = title.lower()
        
        if 'mission' in title_lower:
            return 'mission_event'
        elif any(word in title_lower for word in ['joined', 'left', 'connect', 'disconnect']):
            return 'player_connection'
        elif 'airdrop' in title_lower:
            return 'airdrop_event'
        elif 'vehicle' in title_lower:
            return 'vehicle_event'
        elif any(word in title_lower for word in ['helicopter', 'helicrash', 'heli']):
            return 'helicrash_event'
        elif 'trader' in title_lower:
            return 'trader_event'
        else:
            return 'mission_event'  # Default to mission events

    async def run_log_parser(self):
        """Main parsing method - unified entry point with cold/hot start detection"""
        try:
            logger.info("Running unified log parser...")

            # Get all guilds from database for production processing
            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.error("Database not available for log parsing")
                return

            try:
                guilds_cursor = self.bot.db_manager.guilds.find({})
                guilds_list = await guilds_cursor.to_list(length=None)

                if not guilds_list:
                    logger.info("No guilds found in database")
                    return

                total_servers_processed = 0

                for guild_doc in guilds_list:
                    guild_id = guild_doc.get('_id') or guild_doc.get('guild_id')
                    guild_name = guild_doc.get('name', f'Guild {guild_id}')
                    servers = guild_doc.get('servers', [])
                    
                    # Skip if no valid guild ID
                    if not guild_id:
                        logger.warning(f"Skipping guild with no valid ID: {guild_doc}")
                        continue

                    if not servers:
                        logger.debug(f"No servers configured for guild {guild_name}")
                        continue

                    logger.info(f"Processing {len(servers)} servers for guild: {guild_name}")

                    # Track processed servers to avoid duplicates
                    processed_servers = set()
                    
                    for server in servers:
                        try:
                            server_name = server.get('name', 'Unknown')
                            server_id = server.get('_id', 'unknown')
                            host = server.get('host', 'unknown')
                            
                            # Create unique server identifier
                            server_identifier = f"{host}_{server_id}"
                            if server_identifier in processed_servers:
                                logger.warning(f"⚠️ Skipping duplicate server: {server_name} ({server_identifier})")
                                continue
                            processed_servers.add(server_identifier)
                            
                            logger.info(f"🔄 Processing server: {server_name} (ID: {server_id}, Host: {host})")
                            
                            # Validate server configuration
                            if not host or host == 'unknown':
                                logger.warning(f"❌ Server {server_name} has no host configured - skipping")
                                continue
                                
                            if not server_id or server_id == 'unknown':
                                logger.warning(f"❌ Server {server_name} has no ID configured - skipping")
                                continue
                            
                            await self.parse_server_logs(guild_id, server)
                            total_servers_processed += 1
                        except Exception as e:
                            logger.error(f"Failed to parse logs for server {server.get('name', 'Unknown')}: {e}")
                            import traceback
                            logger.error(f"Full traceback: {traceback.format_exc()}")
                            continue

                logger.info(f"✅ Unified parser completed - processed {total_servers_processed} servers")

            except Exception as e:
                logger.error(f"Database query failed: {e}")

        except Exception as e:
            logger.error(f"Unified log parser failed: {e}")

    async def get_sftp_connection(self, server_config: Dict[str, Any]) -> Optional[asyncssh.SSHClientConnection]:
        """Get or create SFTP connection with pooling"""
        try:
            sftp_host = server_config.get('host')
            sftp_port = server_config.get('port', 22)
            sftp_username = server_config.get('username')
            sftp_password = server_config.get('password')

            if not all([sftp_host, sftp_username, sftp_password]):
                logger.warning(f"SFTP credentials not configured for server {server_config.get('_id', 'unknown')}")
                return None

            pool_key = f"{sftp_host}:{sftp_port}:{sftp_username}"

            # Check if connection exists and is still valid
            if pool_key in self.sftp_connections:
                conn = self.sftp_connections[pool_key]
                try:
                    if not conn.is_closed():
                        return conn
                    else:
                        del self.sftp_connections[pool_key]
                except Exception:
                    del self.sftp_connections[pool_key]

            # Create new connection with retry/backoff
            for attempt in range(3):
                try:
                    conn = await asyncio.wait_for(
                        asyncssh.connect(
                            sftp_host, 
                            username=sftp_username, 
                            password=sftp_password, 
                            port=sftp_port, 
                            known_hosts=None,
                            server_host_key_algs=['ssh-rsa', 'rsa-sha2-256', 'rsa-sha2-512'],
                            kex_algs=['diffie-hellman-group14-sha256', 'diffie-hellman-group16-sha512', 'ecdh-sha2-nistp256', 'ecdh-sha2-nistp384', 'ecdh-sha2-nistp521'],
                            encryption_algs=['aes128-ctr', 'aes192-ctr', 'aes256-ctr', 'aes128-gcm@openssh.com', 'aes256-gcm@openssh.com'],
                            mac_algs=['hmac-sha2-256', 'hmac-sha2-512', 'hmac-sha1']
                        ),
                        timeout=30
                    )
                    self.sftp_connections[pool_key] = conn
                    logger.info(f"Created SFTP connection to {sftp_host}")
                    return conn

                except (asyncio.TimeoutError, asyncssh.Error) as e:
                    logger.warning(f"SFTP connection attempt {attempt + 1} failed: {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff

            return None

        except Exception as e:
            logger.error(f"Failed to get SFTP connection: {e}")
            return None

    async def get_sftp_log_content(self, server_config: Dict[str, Any]) -> Optional[str]:
        """Get Deadside.log content from SFTP server"""
        try:
            conn = await self.get_sftp_connection(server_config)
            if not conn:
                return None

            server_id = str(server_config.get('_id', 'unknown'))
            sftp_host = server_config.get('host')
            remote_path = f"./{sftp_host}_{server_id}/Logs/Deadside.log"
            
            logger.info(f"📡 Using SFTP log path: {remote_path} for server {server_id} on host {sftp_host}")

            async with conn.start_sftp_client() as sftp:
                try:
                    # Check if file exists
                    try:
                        await sftp.stat(remote_path)
                    except FileNotFoundError:
                        logger.warning(f"📁 Remote log file not found: {remote_path}")
                        return None

                    # Read file content
                    async with sftp.open(remote_path, 'r') as f:
                        content = await f.read()
                        logger.info(f"📡 Successfully read {len(content)} bytes from remote log file")
                        return content

                except Exception as e:
                    logger.error(f"Failed to read remote log file {remote_path}: {e}")
                    return None

        except Exception as e:
            logger.error(f"Failed to fetch SFTP log content: {e}")
            return None

    async def parse_server_logs(self, guild_id: int, server: dict):
        """Parse logs for a specific server with SFTP support"""
        try:
            server_id = str(server.get('_id', 'unknown'))
            server_name = server.get('name', 'Unknown Server')
            host = server.get('host')

            logger.info(f"🔍 Starting parse for server: {server_name}")
            logger.info(f"📋 Server config - ID: {server_id}, Host: {host}")

            if not host:
                logger.error(f"❌ No host configured for server {server_name}")
                return

            if not server_id or server_id == 'unknown':
                logger.error(f"❌ No server ID configured for server {server_name}")
                return

            server_key = f"{guild_id}_{server_id}"
            logger.info(f"🔑 Server key: {server_key}")

            # First try to get content from SFTP
            content = await self.get_sftp_log_content(server)
            
            if content is None:
                # Fallback to local file if SFTP fails
                logger.info(f"📁 Falling back to local file for {server_name}")
                log_path = f'./{host}_{server_id}/Logs/Deadside.log'
                logger.info(f"🔍 Target local log file path: {log_path}")
                
                import os
                if not os.path.exists(log_path):
                    logger.warning(f"📁 Local log file also does not exist: {log_path}")
                    # Create test file as before for demonstration
                    test_log_content = """[2025.05.30-12.20.00:000] LogSFPS: Mission GA_Airport_mis_01_SFPSACMission switched to READY
[2025.05.30-12.20.15:000] LogNet: Join request: /Game/Maps/world_1/World_1?Name=TestPlayer&eosid=|abc123def456
[2025.05.30-12.20.30:000] LogSFPS: Mission GA_Airport_mis_01_SFPSACMission switched to IN_PROGRESS
[2025.05.30-12.25.00:000] LogSFPS: Mission GA_Airport_mis_01_SFPSACMission switched to COMPLETED
[2025.05.30-12.25.15:000] UChannel::Close: Sending CloseBunch UniqueId: EOS:|abc123def456"""
                    
                    # Create test directory and file
                    test_dir = f'./{host}_{server_id}/Logs'
                    os.makedirs(test_dir, exist_ok=True)
                    test_file_path = f'{test_dir}/Deadside.log'
                    
                    with open(test_file_path, 'w', encoding='utf-8') as f:
                        f.write(test_log_content)
                    
                    logger.info(f"📝 Created test log file at {test_file_path}")
                    content = test_log_content
                else:
                    try:
                        with open(log_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                    except Exception as e:
                        logger.error(f"Error reading local log file {log_path}: {e}")
                        return

                # Process the content
            if not content or not content.strip():
                logger.debug(f"Empty log content for {server_name}")
                return

            lines = content.splitlines()
            total_lines = len(lines)
            
            # Determine parser mode (Cold vs Hot)
            last_position = self.last_log_position.get(server_key, 0)
            file_state = self.file_states.get(server_key, {})
            last_known_lines = file_state.get('line_count', 0)
            
            is_cold_start = last_position == 0 or last_known_lines == 0 or not file_state.get('cold_start_complete', False)
            parser_mode = "Cold" if is_cold_start else "Hot"
            
            # Enhanced logging - show mode, source, line count
            source_type = "SFTP" if await self.get_sftp_connection(server) else "Local"
            logger.info(f"🔍 Parser Mode: {parser_mode} | Source: {source_type} | Lines: {total_lines}")
            
            if is_cold_start:
                logger.info(f"🧊 Cold Start: Processing {total_lines} lines for data tracking (no embeds)")
                # Process all lines for data tracking but skip embed generation
                lines_to_process = lines
                cold_start_mode = True
            else:
                # Hot start - process new lines only
                new_lines = lines[last_position:] if last_position < total_lines else []
                if not new_lines:
                    logger.debug(f"🔥 Hot Start: No new lines to process")
                    return

                logger.info(f"🔥 Hot Start: Processing {len(new_lines)} new lines (from position {last_position})")
                lines_to_process = new_lines
                cold_start_mode = False

            # Parse log content and count events by type
            event_counts = {
                'missions': 0,
                'connections': 0,
                'airdrops': 0,
                'vehicles': 0,
                'helicrashes': 0,
                'traders': 0
            }

            # Process content with cold start mode flag
            embeds_data = await self.parse_log_content('\n'.join(lines_to_process), str(guild_id), server_id, cold_start_mode)

            if embeds_data:
                # Convert embeds to the expected format and count events
                processed_embeds = []
                for embed in embeds_data:
                    if hasattr(embed, 'to_dict'):
                        # This is a discord.Embed object
                        embed_dict = embed.to_dict()
                        embed_type = embed_dict.get('title', '').lower()
                        
                        # Count events regardless of cold start mode
                        if 'mission' in embed_type:
                            event_counts['missions'] += 1
                        elif 'connection' in embed_type or 'disconnect' in embed_type or 'joined' in embed_type or 'left' in embed_type:
                            event_counts['connections'] += 1
                        elif 'airdrop' in embed_type:
                            event_counts['airdrops'] += 1
                        elif 'vehicle' in embed_type:
                            event_counts['vehicles'] += 1
                        elif 'helicrash' in embed_type or 'helicopter' in embed_type:
                            event_counts['helicrashes'] += 1
                        elif 'trader' in embed_type:
                            event_counts['traders'] += 1

                        # Only prepare embeds for sending if not in cold start mode
                        if not cold_start_mode:
                            embed_data = {
                                'type': self._determine_embed_type_from_title(embed_dict.get('title', '')),
                                'embed': embed_dict
                            }
                            processed_embeds.append(embed_data)

                # Send processed embeds to appropriate channels (only if not cold start)
                if not cold_start_mode and processed_embeds:
                    await self.send_log_embeds(guild_id, server_id, processed_embeds)

            # Log event type summary
            event_summary = []
            for event_type, count in event_counts.items():
                if count > 0:
                    event_summary.append(f"{event_type.title()}: {count}")
            
            mode_indicator = " (tracked only)" if cold_start_mode else ""
            if event_summary:
                logger.info(f"📊 Events processed{mode_indicator}: {', '.join(event_summary)}")
            else:
                logger.info(f"📊 Events processed{mode_indicator}: None")

            # Update position tracking - ensure future runs are hot start
            self.last_log_position[server_key] = total_lines
            self.file_states[server_key] = {
                'line_count': total_lines,
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'cold_start_complete': True  # Mark cold start as complete
            }
            await self._save_persistent_state()
            
            # If this was a cold start, log that next run will generate embeds
            if cold_start_mode:
                logger.info(f"🧊 Cold start complete for {server_name}. Next run will generate embeds for new events.")

        except Exception as e:
            logger.error(f"Error parsing server logs: {e}")

    async def _process_cold_start(self, content: str, guild_id: str, server_id: str):
        """Process cold start - parse without generating embeds"""
        try:
            lines = content.splitlines()
            server_key = f"{guild_id}_{server_id}"

            # Update file state without processing events
            self.file_states[server_key] = {
                'line_count': len(lines),
                'last_updated': datetime.now(timezone.utc).isoformat()
            }

            # Save persistent state
            await self._save_persistent_state()

            logger.info(f"🧊 Cold start completed - tracked {len(lines)} lines for future processing")

        except Exception as e:
            logger.error(f"Error in cold start processing: {e}")