package sync

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestStore_SetAndGetLastProcessedTick(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	var epoch uint32 = 100
	var tick uint32 = 200

	err = store.SetLastProcessedTick(epoch, tick)
	require.NoError(t, err)

	retrievedTick, err := store.GetLastProcessedTick(epoch)
	require.NoError(t, err)
	require.Equal(t, tick, retrievedTick)
}

func TestStore_GetLastProcessedTickNotSet(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	_, err = store.GetLastProcessedTick(999)
	require.Error(t, err)
	require.Equal(t, ErrNotFound, err)
}

func TestStore_OverwriteLastProcessedTick(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	var epoch uint32 = 123
	var initialTick uint32 = 456
	var newTick uint32 = 789

	err = store.SetLastProcessedTick(epoch, initialTick)
	require.NoError(t, err)

	retrievedTick, err := store.GetLastProcessedTick(epoch)
	require.NoError(t, err)
	require.Equal(t, initialTick, retrievedTick)

	err = store.SetLastProcessedTick(epoch, newTick)
	require.NoError(t, err)

	retrievedTick, err = store.GetLastProcessedTick(epoch)
	require.NoError(t, err)
	require.Equal(t, newTick, retrievedTick)
}

func TestStore_MultipleEpochs(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	var epoch1 uint32 = 1
	var tick1 uint32 = 100
	var epoch2 uint32 = 2
	var tick2 uint32 = 200

	err = store.SetLastProcessedTick(epoch1, tick1)
	require.NoError(t, err)

	err = store.SetLastProcessedTick(epoch2, tick2)
	require.NoError(t, err)

	retrievedTick1, err := store.GetLastProcessedTick(epoch1)
	require.NoError(t, err)
	require.Equal(t, tick1, retrievedTick1)

	retrievedTick2, err := store.GetLastProcessedTick(epoch2)
	require.NoError(t, err)
	require.Equal(t, tick2, retrievedTick2)
}
